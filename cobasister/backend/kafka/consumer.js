const { Kafka } = require('kafkajs');
const pool = require('../models/db');
require('dotenv').config();

const kafka = new Kafka({
  clientId: 'admin-logger',
  brokers: [process.env.KAFKA_BROKER],
});

const consumer = kafka.consumer({ groupId: 'admin-group' });

const startConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'admin-dashboard-log', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { event_type, description } = JSON.parse(message.value.toString());
      await pool.query(
        'INSERT INTO admin_logs (event_type, description) VALUES (?, ?)',
        [event_type, description]
      );
      console.log('Log inserted from Kafka');
    },
  });
};

startConsumer().catch(console.error);
