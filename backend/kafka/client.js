const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || 'order-system',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

async function createProducer() {
  const producer = kafka.producer();
  await producer.connect();
  return producer;
}

async function createConsumer(groupId) {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  return consumer;
}

module.exports = {
  kafka,
  createProducer,
  createConsumer
};
