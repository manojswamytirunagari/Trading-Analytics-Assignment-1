require("dotenv").config();
const fs = require("fs");
const csv = require("csv-parser");
const { Kafka, Partitioners } = require("kafkajs");

// Validate environment variables early
const requiredEnvs = [
  "REDPANDA_BROKER",
  "REDPANDA_USERNAME",
  "REDPANDA_PASSWORD",
  "REDPANDA_MECHANISM",
  "REDPANDA_SECURITY_PROTOCOL",
];
for (const key of requiredEnvs) {
  if (!process.env[key]) {
    console.error(`Missing environment variable: ${key}`);
    process.exit(1);
  }
}

const kafka = new Kafka({
  clientId: "trading-analytics-producer",
  brokers: [process.env.REDPANDA_BROKER],
  ssl: process.env.REDPANDA_SECURITY_PROTOCOL.toLowerCase() === "sasl_ssl",
  sasl: {
    mechanism: process.env.REDPANDA_MECHANISM, // scram-sha-256
    username: process.env.REDPANDA_USERNAME,
    password: process.env.REDPANDA_PASSWORD,
  },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner, // silence warning
});

const run = async () => {
  try {
    await producer.connect();
    console.log("Producer connected");

    return new Promise((resolve, reject) => {
      const results = [];

      fs.createReadStream("trades_data.csv")
        .pipe(csv())
        .on("data", (row) => {
          results.push(row);
        })
        .on("end", async () => {
          console.log(`CSV file processed: ${results.length} records`);

          try {
            for (const record of results) {
              await producer.send({
                topic: "trade-data", // must exist in Redpanda
                messages: [{ value: JSON.stringify(record) }],
              });
            }

            console.log("All messages sent successfully");
            await producer.disconnect();
            resolve();
          } catch (err) {
            console.error("Error sending messages:", err);
            reject(err);
          }
        });
    });
  } catch (err) {
    console.error("Failed to connect producer:", err);
    process.exit(1);
  }
};

run().catch(console.error);
