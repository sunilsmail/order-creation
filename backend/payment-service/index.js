const { createConsumer, createProducer } = require('../kafka/client');
const { ORDER_EVENTS } = require('../kafka/topics');

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
  const consumer = await createConsumer('payment-service-group');
  const producer = await createProducer();

  await consumer.subscribe({ topic: ORDER_EVENTS, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      if (event.eventType !== 'PAYMENT_REQUEST') return;

      await wait(1000);

      const success = Math.random() > 0.2;
      const resultType = success ? 'PAYMENT_SUCCESS' : 'PAYMENT_FAILED';

      await publishEvent(producer, {
        eventType: resultType,
        orderId: event.orderId,
        userId: event.userId,
        message: success
          ? `Payment completed for ${event.orderId}`
          : `Payment failed for ${event.orderId}`,
        timestamp: new Date().toISOString()
      });
    }
  });

  console.log('Payment Service listening for payment requests...');
}

start().catch((error) => {
  console.error('Payment service failed:', error);
  process.exit(1);
});
