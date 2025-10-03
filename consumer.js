require("dotenv").config();
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "trading-analytics-consumer",
  brokers: [process.env.REDPANDA_BROKER],
  ssl: true,
  sasl: {
    mechanism: process.env.REDPANDA_MECHANISM, // SCRAM-SHA-256
    username: process.env.REDPANDA_USERNAME,
    password: process.env.REDPANDA_PASSWORD,
  },
});

const consumer = kafka.consumer({ groupId: "trading-analytics-group" });

const run = async () => {
  await consumer.connect();
  console.log("Consumer connected");

  // Subscribe to all topics or specific ones
  await consumer.subscribe({ topic: "trade-data", fromBeginning: true });
  await consumer.subscribe({ topic: "rsi-data", fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = message.value.toString();
        let parsed;
        try {
          parsed = JSON.parse(value);
        } catch {
          parsed = value; // If not JSON, just print raw
        }

        console.log(`[${topic}] Partition: ${partition} =>`, parsed);
      } catch (err) {
        console.error("Error processing message:", err);
      }
    },
  });
};

run().catch(console.error);
