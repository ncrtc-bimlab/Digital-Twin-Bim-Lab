// Shared Kafka connection + topic names. Every service imports this.
const { Kafka, logLevel } = require("kafkajs");

const brokers = (process.env.KAFKA_BROKERS || "localhost:9094").split(",");

const kafka = new Kafka({
  clientId: "ncrtc-twin",
  brokers,
  logLevel: logLevel.WARN,
  retry: { initialRetryTime: 300, retries: 10 },
});

const TOPICS = {
  RAW: "telemetry.raw",           // validated readings, the single source of truth
  REJECTED: "telemetry.rejected", // bad messages, kept so we can investigate (dead-letter)
};

module.exports = { kafka, TOPICS };
