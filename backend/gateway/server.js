const express = require('express');
const http = require('http');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const { Server } = require('socket.io');
const { createProducer, createConsumer } = require('../kafka/client');
const { ORDER_EVENTS } = require('../kafka/topics');

const PORT = process.env.GATEWAY_PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || 'super-secret-key';

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*'
  }
});

const userSockets = new Map(); // userId -> Set<socketId>

function emitOrderUpdate(update) {
  const roomName = `order:${update.orderId}`;
  io.to(roomName).emit('order-update', update);

  const sockets = userSockets.get(update.userId);
  if (sockets) {
    for (const socketId of sockets) {
      io.to(socketId).emit('order-update', update);
    }
  }
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
  res.json({ token });
});

app.post('/api/orders', authMiddleware, async (req, res) => {
  const { items = [], total = 100, location = 'US' } = req.body || {};
  const orderId = `ord_${Date.now()}_${Math.floor(Math.random() * 100000)}`;

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

    emitOrderUpdate({
      orderId,
      userId: req.user.userId,
      status: 'ORDER_CREATED',
      message: 'Order created. Running validations...'
    });

    return res.status(202).json({ orderId, status: 'PROCESSING' });
  } catch (error) {
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
  if (!userSockets.has(userId)) {
    userSockets.set(userId, new Set());
  }
  userSockets.get(userId).add(socket.id);

  socket.on('join-order', ({ orderId }) => {
    socket.join(`order:${orderId}`);
    socket.emit('order-update', {
      orderId,
      userId,
      status: 'ROOM_JOINED',
      message: `Subscribed to updates for ${orderId}`
    });
  });

  socket.on('disconnect', () => {
    const sockets = userSockets.get(userId);
    if (!sockets) return;
    sockets.delete(socket.id);
    if (sockets.size === 0) {
      userSockets.delete(userId);
    }
  });
});

async function start() {
  const producer = await createProducer();
  const consumer = await createConsumer('gateway-order-events-group');

  app.locals.producer = producer;

  await consumer.subscribe({ topic: ORDER_EVENTS, fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const event = JSON.parse(message.value.toString());

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
          return;
        }

        emitOrderUpdate({
          orderId: event.orderId,
          userId: event.userId,
          status: event.eventType,
          message: event.message || event.eventType
        });
      } catch (error) {
        console.error('Gateway consume error:', error.message);
      }
    }
  });

  server.listen(PORT, () => {
    console.log(`Gateway listening on http://localhost:${PORT}`);
  });
}

start().catch((error) => {
  console.error('Gateway startup failed:', error);
  process.exit(1);
});
