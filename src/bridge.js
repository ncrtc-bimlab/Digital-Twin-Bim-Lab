// MQTT -> Kafka bridge.
// Its only job: take every reading from the broker, check it, and write it into Kafka.
// It tells MQTT "got it" ONLY AFTER Kafka has confirmed the write. So a message is
// never acknowledged until it is safely stored in the log.

const mqtt = require("mqtt");
const { kafka, TOPICS } = require("./kafka");
const { validate } = require("./validate");

const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";

// idempotent = if a retry happens, Kafka will not store the same message twice
const producer = kafka.producer({ idempotent: true });
const stats = { forwarded: 0, rejected: 0, kafka_retries: 0 };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Keeps trying until Kafka accepts the message. While it keeps failing we also
// stop acknowledging MQTT messages, so the MQTT broker holds them for us (backpressure).
async function sendWithRetry(topic, key, value) {
  let delay = 500;
  for (;;) {
    try {
      await producer.send({ topic, acks: -1, messages: [{ key, value }] }); // -1 = all replicas confirmed
      return;
    } catch (e) {
      stats.kafka_retries++;
      console.error(`Kafka send failed (${e.message}). Retrying in ${delay} ms`);
      await sleep(delay);
      delay = Math.min(delay * 2, 5000);
    }
  }
}

async function forward(packet) {
  const text = packet.payload.toString();
  let msg = null;
  try {
    msg = JSON.parse(text);
  } catch {
    /* handled below */
  }

  const problem =
    msg === null || typeof msg !== "object" || Array.isArray(msg)
      ? "not a valid JSON object"
      : validate(msg);

  if (problem) {
    stats.rejected++;
    console.error(`REJECTED (${problem}) on ${packet.topic}`);
    await sendWithRetry(
      TOPICS.REJECTED,
      null,
      JSON.stringify({ reason: problem, mqtt_topic: packet.topic, payload: text.slice(0, 1000), at: new Date().toISOString() })
    );
    return;
  }

  // KEY = sensor_id. Kafka sends the same key to the same partition, so every reading
  // of one sensor stays in order, while different sensors spread across partitions.
  await sendWithRetry(TOPICS.RAW, String(msg.sensor_id), JSON.stringify(msg));
  stats.forwarded++;
}

(async () => {
  await producer.connect();
  console.log("Kafka connected");

  // Durable session: if the bridge is down, the MQTT broker keeps the messages.
  const client = mqtt.connect(MQTT_URL, { clientId: "twin-bridge-1", clean: false });

  // The MQTT library sends its acknowledgement when we call callback().
  // We call it only after forward() has finished, i.e. after Kafka confirmed.
  client.handleMessage = (packet, callback) => {
    forward(packet)
      .catch((e) => console.error("Dropped a message:", e.message))
      .finally(() => callback());
  };

  client.on("connect", () => {
    console.log("MQTT connected");
    client.subscribe("twin/+/+/+/telemetry", { qos: 1 });
  });
  client.on("error", (e) => console.error("MQTT error:", e.message));

  setInterval(() => console.log("bridge", stats), 10000);

  process.on("SIGINT", async () => {
    console.log("Shutting down...");
    client.end();
    await producer.disconnect();
    process.exit(0);
  });
})().catch((e) => {
  console.error("Bridge failed to start:", e.message);
  process.exit(1);
});
