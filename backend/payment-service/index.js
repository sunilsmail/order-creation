const { createConsumer, createProducer } = require('../kafka/client');
const { ORDER_EVENTS } = require('../kafka/topics');

// Logger utility
const logger = {
  info: (msg, data) => console.log(`[${new Date().toISOString()}] [PAYMENT-SERVICE-INFO] ${msg}`, data ? JSON.stringify(data) : ''),
  error: (msg, data) => console.error(`[${new Date().toISOString()}] [PAYMENT-SERVICE-ERROR] ${msg}`, data ? JSON.stringify(data) : ''),
  warn: (msg, data) => console.warn(`[${new Date().toISOString()}] [PAYMENT-SERVICE-WARN] ${msg}`, data ? JSON.stringify(data) : '')
};

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function publishEvent(producer, event) {
  await producer.send({
    topic: ORDER_EVENTS,
    messages: [{ key: event.orderId, value: JSON.stringify(event) }]
  });
}

async function start() {
  logger.info('Payment Service starting...');
  
  const consumer = await createConsumer('payment-service-group');
  const producer = await createProducer();

  logger.info('Kafka producer and consumer initialized');
  
  await consumer.subscribe({ topic: ORDER_EVENTS, fromBeginning: false });
  logger.info('Subscribed to Kafka topic', { topic: ORDER_EVENTS });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      
      if (event.eventType !== 'PAYMENT_REQUEST') {
        return;
      }

      logger.info('Payment request received', { orderId: event.orderId, userId: event.userId });

      await wait(1000);

      const success = Math.random() > 0.2;
      const resultType = success ? 'PAYMENT_SUCCESS' : 'PAYMENT_FAILED';

      logger.info('Processing payment', { orderId: event.orderId, resultType, success });

      await publishEvent(producer, {
        eventType: resultType,
        orderId: event.orderId,
        userId: event.userId,
        message: success
          ? `Payment completed for ${event.orderId}`
          : `Payment failed for ${event.orderId}`,
        timestamp: new Date().toISOString()
      });

      logger.info('Payment result published', { orderId: event.orderId, resultType });
    }
  });

  logger.info('Payment Service listening for payment requests');
}

start().catch((error) => {
  logger.error('Payment service failed', { error: error.message });
  process.exit(1);
});
