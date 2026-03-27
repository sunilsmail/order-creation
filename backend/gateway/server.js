const express = require('express');
const http = require('http');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { Server } = require('socket.io');
const { createProducer, createConsumer } = require('../kafka/client');
const { ORDER_EVENTS } = require('../kafka/topics');

const PORT = process.env.GATEWAY_PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'super-secret-key';

// Logger utility
const logger = {
  info: (msg, data) => console.log(`[${new Date().toISOString()}] [GATEWAY-INFO] ${msg}`, data ? JSON.stringify(data) : ''),
  error: (msg, data) => console.error(`[${new Date().toISOString()}] [GATEWAY-ERROR] ${msg}`, data ? JSON.stringify(data) : ''),
  warn: (msg, data) => console.warn(`[${new Date().toISOString()}] [GATEWAY-WARN] ${msg}`, data ? JSON.stringify(data) : '')
};

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

const userSockets = new Map(); // userId -> Set<socketId>
const paymentRetryState = new Map(); // orderId -> { retryCount, userId, maxRetries }

function getExponentialBackoffDelay(retryCount) {
  // Exponential backoff: 1s, 2s, 4s for retries 0, 1, 2
  return 1000 * Math.pow(2, retryCount);
}

function emitOrderUpdate(update) {
  const roomName = `order:${update.orderId}`;
  // Emit only to the room - clients are subscribed via room membership
  io.to(roomName).emit('order-update', update);
  logger.info('Emitting order update to room', { orderId: update.orderId, roomName, status: update.status });
}

function authMiddleware(req, res, next) {
  const authHeader = req.headers.authorization || '';
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null;

  if (!token) {
    return res.status(401).json({ message: 'Missing Bearer token' });
  }

  try {
    req.user = jwt.verify(token, JWT_SECRET);
    return next();
  } catch (error) {
    return res.status(401).json({ message: 'Invalid token' });
  }
}

app.get('/api/token/:userId', (req, res) => {
  const token = jwt.sign({ userId: req.params.userId }, JWT_SECRET, { expiresIn: '24h' });
  logger.info('Token generated', { userId: req.params.userId });
  res.json({ token });
});

app.post('/api/orders', authMiddleware, async (req, res) => {
  const { items = [], total = 100, location = 'US' } = req.body || {};
  const orderId = `ord_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

  logger.info('Order creation initiated', { orderId, userId: req.user.userId, items, total, location });

  const event = {
    eventType: 'ORDER_CREATED',
    orderId,
    userId: req.user.userId,
    payload: { items, total, location },
    timestamp: new Date().toISOString()
  };

  try {
    await req.app.locals.producer.send({
      topic: ORDER_EVENTS,
      messages: [{ key: orderId, value: JSON.stringify(event) }]
    });

    logger.info('Order event published to Kafka', { orderId, eventType: 'ORDER_CREATED' });

    emitOrderUpdate({
      orderId,
      userId: req.user.userId,
      status: 'ORDER_CREATED',
      message: 'Order created. Running validations...'
    });

    return res.status(202).json({ orderId, status: 'PROCESSING' });
  } catch (error) {
    logger.error('Failed to publish order event', { orderId, error: error.message });
    return res.status(500).json({ message: 'Failed to publish order event', error: error.message });
  }
});

io.use((socket, next) => {
  const token = socket.handshake.auth?.token || socket.handshake.query?.token;
  if (!token) {
    return next(new Error('Missing token'));
  }

  try {
    const payload = jwt.verify(token, JWT_SECRET);
    socket.user = payload;
    return next();
  } catch (error) {
    return next(new Error('Unauthorized'));
  }
});

io.on('connection', (socket) => {
  const { userId } = socket.user;
  logger.info('Socket connection established', { socketId: socket.id, userId });

  if (!userSockets.has(userId)) {
    userSockets.set(userId, new Set());
  }
  userSockets.get(userId).add(socket.id);

  socket.on('join-order', ({ orderId }) => {
    socket.join(`order:${orderId}`);
    logger.info('User joined order room', { socketId: socket.id, userId, orderId });
    socket.emit('order-update', {
      orderId,
      userId,
      status: 'ROOM_JOINED',
      message: `Subscribed to updates for ${orderId}`
    });
  });

  socket.on('disconnect', () => {
    logger.info('Socket disconnected', { socketId: socket.id, userId });
    const sockets = userSockets.get(userId);
    if (!sockets) return;
    sockets.delete(socket.id);
    if (sockets.size === 0) {
      userSockets.delete(userId);
    }
  });
});

async function start() {
  logger.info('Gateway service starting...');
  
  const producer = await createProducer();
  const consumer = await createConsumer('gateway-order-events-group');

  logger.info('Kafka producer and consumer initialized');

  app.locals.producer = producer;

  await consumer.subscribe({ topic: ORDER_EVENTS, fromBeginning: false });
  logger.info('Subscribed to Kafka topic', { topic: ORDER_EVENTS });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const event = JSON.parse(message.value.toString());
        logger.info('Event received from Kafka', { eventType: event.eventType, orderId: event.orderId });

        const statusEvents = new Set([
          'PRODUCT_AVAILABLE',
          'PRODUCT_UNAVAILABLE',
          'PRICE_CONFIRMED',
          'PRICE_MISMATCH',
          'LOCATION_VALID',
          'LOCATION_INVALID',
          'PAYMENT_SUCCESS',
          'PAYMENT_FAILED',
          'ORDER_FAILED'
        ]);

        if (!statusEvents.has(event.eventType)) {
          logger.warn('Unknown event type, skipping', { eventType: event.eventType });
          return;
        }

        // Handle payment failure retries
        if (event.eventType === 'PAYMENT_FAILED') {
          if (!paymentRetryState.has(event.orderId)) {
            paymentRetryState.set(event.orderId, { retryCount: 0, userId: event.userId, maxRetries: 3 });
          }
          
          const retryData = paymentRetryState.get(event.orderId);
          
          if (retryData.retryCount < retryData.maxRetries) {
            retryData.retryCount += 1;
            const backoffDelay = getExponentialBackoffDelay(retryData.retryCount - 1);
            
            logger.info('Scheduling payment retry', { 
              orderId: event.orderId,
              attemptNumber: retryData.retryCount,
              delayMs: backoffDelay,
              maxRetries: retryData.maxRetries
            });
            
            // Schedule retry with exponential backoff
            setTimeout(async () => {
              logger.info('Publishing payment retry', { orderId: event.orderId, attemptNumber: retryData.retryCount });
              await producer.send({
                topic: ORDER_EVENTS,
                messages: [{
                  key: event.orderId,
                  value: JSON.stringify({
                    eventType: 'PAYMENT_REQUEST',
                    orderId: event.orderId,
                    userId: retryData.userId,
                    message: `Payment retry attempt ${retryData.retryCount}/${retryData.maxRetries} for ${event.orderId}`,
                    timestamp: new Date().toISOString()
                  })
                }]
              });
            }, backoffDelay);

            // Emit intermediate status to clients
            logger.info('Emitting payment retry status to clients', { orderId: event.orderId, attempt: retryData.retryCount });
            emitOrderUpdate({
              orderId: event.orderId,
              userId: event.userId,
              status: 'PAYMENT_RETRY',
              message: `Retrying payment (attempt ${retryData.retryCount}/${retryData.maxRetries})...`
            });
            return; // Don't emit PAYMENT_FAILED to clients, emit retry status instead
          } else {
            // Max retries exceeded
            logger.error('Max payment retries exceeded', { orderId: event.orderId, maxRetries: retryData.maxRetries });
            paymentRetryState.delete(event.orderId);
            // Emit to clients that order failed
            emitOrderUpdate({
              orderId: event.orderId,
              userId: event.userId,
              status: 'ORDER_FAILED',
              message: `Payment failed after ${retryData.maxRetries} attempts`
            });
            return;
          }
        }

        logger.info('Emitting order update to clients', { orderId: event.orderId, status: event.eventType });
        emitOrderUpdate({
          orderId: event.orderId,
          userId: event.userId,
          status: event.eventType,
          message: event.message || event.eventType
        });
      } catch (error) {
        logger.error('Error processing Kafka message', { error: error.message });
      }
    }
  });

  server.listen(PORT, () => {
    logger.info('Gateway listening', { port: PORT, url: `http://localhost:${PORT}` });
  });
}

start().catch((error) => {
  logger.error('Gateway startup failed', { error: error.message });
  process.exit(1);
});
