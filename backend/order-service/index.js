const { createConsumer, createProducer } = require('../kafka/client');
const { ORDER_EVENTS } = require('../kafka/topics');

const validationState = new Map();

function randomDelay(min = 300, max = 1200) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
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
  const consumer = await createConsumer('order-service-group');
  const producer = await createProducer();

  await consumer.subscribe({ topic: ORDER_EVENTS, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const { eventType, orderId, userId } = event;

      if (eventType === 'ORDER_CREATED') {
        validationState.set(orderId, {
          product: false,
          price: false,
          location: false,
          completed: false,
          userId
        });

        const checks = ['PRODUCT_CHECK', 'PRICE_CHECK', 'LOCATION_CHECK'];
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
        await processCheck(producer, eventType, orderId, userId);
      }

      if (eventType === 'PRODUCT_AVAILABLE') {
        updateValidation(orderId, 'product', true);
        await maybeRequestPayment(producer, orderId);
      }

      if (eventType === 'PRICE_CONFIRMED') {
        updateValidation(orderId, 'price', true);
        await maybeRequestPayment(producer, orderId);
      }

      if (eventType === 'LOCATION_VALID') {
        updateValidation(orderId, 'location', true);
        await maybeRequestPayment(producer, orderId);
      }

      if (
        eventType === 'PRODUCT_UNAVAILABLE' ||
        eventType === 'PRICE_MISMATCH' ||
        eventType === 'LOCATION_INVALID'
      ) {
        await publishEvent(producer, {
          eventType: 'ORDER_FAILED',
          orderId,
          userId,
          message: `Order ${orderId} failed because ${eventType}`,
          timestamp: new Date().toISOString()
        });
      }
    }
  });

  console.log('Order Service listening for order events...');
}

start().catch((error) => {
  console.error('Order service failed:', error);
  process.exit(1);
});
