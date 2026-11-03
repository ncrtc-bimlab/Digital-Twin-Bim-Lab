// Listener: subscribes to every telemetry message and validates it.
// Next days: this logic moves into a Kafka consumer that writes to TimescaleDB + Redis.

const mqtt = require("mqtt");

const BROKER = process.env.MQTT_URL || "mqtt://localhost:1883";
const REQUIRED = ["sensor_id", "station", "zone", "metric", "value", "unit", "timestamp"];

const client = mqtt.connect(BROKER);

client.on("connect", () => {
  console.log(`Connected to ${BROKER}`);
  // '+' = one level wildcard, '#' = everything below
  client.subscribe("twin/+/+/+/telemetry", { qos: 1 }, (err) => {
    if (err) return console.error("Subscribe failed:", err.message);
    console.log("Listening on twin/+/+/+/telemetry ...");
  });
});

client.on("message", (topic, payload) => {
  let msg;
  try {
    msg = JSON.parse(payload.toString());
  } catch {
    return console.error("BAD JSON on", topic);
  }

  const missing = REQUIRED.filter((k) => msg[k] === undefined);
  if (missing.length) {
    return console.error("INVALID (missing " + missing.join(", ") + ") on", topic);
  }

  console.log(
    `[${msg.timestamp}] ${msg.station}/${msg.zone}/${msg.sensor_id} ` +
      `${msg.metric} = ${msg.value} ${msg.unit} (via ${msg.protocol || "?"})`
  );
});

client.on("error", (err) => console.error("MQTT error:", err.message));
