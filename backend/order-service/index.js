const { createConsumer, createProducer } = require('../kafka/client');
const { ORDER_EVENTS } = require('../kafka/topics');

// Logger utility
const logger = {
  info: (msg, data) => console.log(`[${new Date().toISOString()}] [ORDER-SERVICE-INFO] ${msg}`, data ? JSON.stringify(data) : ''),
  error: (msg, data) => console.error(`[${new Date().toISOString()}] [ORDER-SERVICE-ERROR] ${msg}`, data ? JSON.stringify(data) : ''),
  warn: (msg, data) => console.warn(`[${new Date().toISOString()}] [ORDER-SERVICE-WARN] ${msg}`, data ? JSON.stringify(data) : '')
};

const validationState = new Map();
const paymentRetryState = new Map(); // Track payment retry attempts

function randomDelay(min = 300, max = 1200) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getExponentialBackoffDelay(retryCount) {
  // Exponential backoff: 1s, 2s, 4s for retries 0, 1, 2
  return 1000 * Math.pow(2, retryCount);
}

async function publishEvent(producer, event) {
  await producer.send({
    topic: ORDER_EVENTS,
    messages: [{ key: event.orderId, value: JSON.stringify(event) }]
  });
}

async function processCheck(producer, eventType, orderId, userId) {
  await new Promise((resolve) => setTimeout(resolve, randomDelay()));

  const successfulEventMap = {
    PRODUCT_CHECK: 'PRODUCT_AVAILABLE',
    PRICE_CHECK: 'PRICE_CONFIRMED',
    LOCATION_CHECK: 'LOCATION_VALID'
  };

  const resultEventType = successfulEventMap[eventType];

  await publishEvent(producer, {
    eventType: resultEventType,
    orderId,
    userId,
    message: `${resultEventType} for ${orderId}`,
    timestamp: new Date().toISOString()
  });
}

function updateValidation(orderId, field, value) {
  if (!validationState.has(orderId)) {
    validationState.set(orderId, {
      product: false,
      price: false,
      location: false,
      completed: false,
      userId: null
    });
  }

  const current = validationState.get(orderId);
  current[field] = value;
}

async function maybeRequestPayment(producer, orderId) {
  const state = validationState.get(orderId);
  if (!state || state.completed) return;

  if (state.product && state.price && state.location) {
    state.completed = true;
    await publishEvent(producer, {
      eventType: 'PAYMENT_REQUEST',
      orderId,
      userId: state.userId,
      message: `All checks passed. Requesting payment for ${orderId}`,
      timestamp: new Date().toISOString()
    });
  }
}

async function start() {
  logger.info('Order Service starting...');
  
  const consumer = await createConsumer('order-service-group');
  const producer = await createProducer();

  logger.info('Kafka producer and consumer initialized');
  
  await consumer.subscribe({ topic: ORDER_EVENTS, fromBeginning: false });
  logger.info('Subscribed to Kafka topic', { topic: ORDER_EVENTS });

  try {
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const event = JSON.parse(message.value.toString());
          const { eventType, orderId, userId } = event;

          logger.info('Event received', { eventType, orderId, userId });

      if (eventType === 'ORDER_CREATED') {
        logger.info('Processing ORDER_CREATED', { orderId, userId });
        
        validationState.set(orderId, {
          product: false,
          price: false,
          location: false,
          completed: false,
          userId
        });

        const checks = ['PRODUCT_CHECK', 'PRICE_CHECK', 'LOCATION_CHECK'];
        logger.info('Triggering validation checks', { orderId, checks });
        
        await Promise.all(
          checks.map((checkType) =>
            publishEvent(producer, {
              eventType: checkType,
              orderId,
              userId,
              message: `Triggered ${checkType}`,
              timestamp: new Date().toISOString()
            })
          )
        );
      }

      if (eventType === 'PRODUCT_CHECK' || eventType === 'PRICE_CHECK' || eventType === 'LOCATION_CHECK') {
        logger.info('Processing check event', { eventType, orderId });
        await processCheck(producer, eventType, orderId, userId);
      }

      if (eventType === 'PRODUCT_AVAILABLE') {
        logger.info('Product available for order', { orderId });
        updateValidation(orderId, 'product', true);
        await maybeRequestPayment(producer, orderId);
      }

      if (eventType === 'PRICE_CONFIRMED') {
        logger.info('Price confirmed for order', { orderId });
        updateValidation(orderId, 'price', true);
        await maybeRequestPayment(producer, orderId);
      }

      if (eventType === 'LOCATION_VALID') {
        logger.info('Location valid for order', { orderId });
        updateValidation(orderId, 'location', true);
        await maybeRequestPayment(producer, orderId);
      }

      if (
        eventType === 'PRODUCT_UNAVAILABLE' ||
        eventType === 'PRICE_MISMATCH' ||
        eventType === 'LOCATION_INVALID'
      ) {
        logger.warn('Order validation failed', { orderId, reason: eventType });
        await publishEvent(producer, {
          eventType: 'ORDER_FAILED',
          orderId,
          userId,
          message: `Order ${orderId} failed because ${eventType}`,
          timestamp: new Date().toISOString()
        });
      }

      if (eventType === 'PAYMENT_FAILED') {
        logger.warn('Payment failed for order', { orderId, userId });
        
        // Track retry attempts
        if (!paymentRetryState.has(orderId)) {
          paymentRetryState.set(orderId, { retryCount: 0, userId });
        }
        
        const retryData = paymentRetryState.get(orderId);
        
        if (retryData.retryCount < 3) {
          retryData.retryCount += 1;
          const backoffDelay = getExponentialBackoffDelay(retryData.retryCount - 1);
          
          logger.info('Scheduling payment retry', { 
            orderId, 
            attemptNumber: retryData.retryCount,
            delayMs: backoffDelay 
          });
          
          // Schedule retry with exponential backoff
          setTimeout(async () => {
            logger.info('Retrying payment request', { orderId, attemptNumber: retryData.retryCount });
            await publishEvent(producer, {
              eventType: 'PAYMENT_REQUEST',
              orderId,
              userId: retryData.userId,
              message: `Payment retry attempt ${retryData.retryCount} for ${orderId}`,
              timestamp: new Date().toISOString()
            });
          }, backoffDelay);
        } else {
          logger.error('Max payment retries exceeded', { orderId, maxRetries: 3 });
          paymentRetryState.delete(orderId);
          await publishEvent(producer, {
            eventType: 'ORDER_FAILED',
            orderId,
            userId,
            message: `Order ${orderId} failed - maximum payment retries exceeded`,
            timestamp: new Date().toISOString()
          });
        }
      }
        } catch (err) {
          logger.error('Error processing message', { error: err.message });
        }
      }
    });
  } catch (err) {
    logger.error('Error in consumer', { error: err.message });
  }

  logger.info('Order Service listening for order events');
}

start().catch((error) => {
  logger.error('Order service failed', { error: error.message });
  process.exit(1);
});
