// Fake sensor simulator.
// Publishes readings in OUR standard message format to MQTT.
// Later, real LoRaWAN / BLE / WiFi adapters will produce this exact same shape.

const mqtt = require("mqtt");

const BROKER = process.env.MQTT_URL || "mqtt://localhost:1883";
const INTERVAL_MS = Number(process.env.INTERVAL_MS || 5000);

// A few pretend sensors. Change/add freely.
const sensors = [
  { sensor_id: "AV070", station: "ANAV", zone: "P123", metric: "temperature", unit: "C",   value: 28, min: 15, max: 45, protocol: "lorawan" },
  { sensor_id: "AV071", station: "ANAV", zone: "P123", metric: "humidity",    unit: "%",   value: 55, min: 20, max: 95, protocol: "lorawan" },
  { sensor_id: "AV072", station: "ANAV", zone: "P123", metric: "co2",         unit: "ppm", value: 600, min: 400, max: 2000, protocol: "ble" },
  { sensor_id: "AS033", station: "ANAV", zone: "F201", metric: "temperature", unit: "C",   value: 30, min: 15, max: 45, protocol: "wifi" },
];

const client = mqtt.connect(BROKER);

client.on("connect", () => {
  console.log(`Connected to ${BROKER}. Publishing every ${INTERVAL_MS} ms. Ctrl+C to stop.`);
  setInterval(tick, INTERVAL_MS);
});

client.on("error", (err) => console.error("MQTT error:", err.message));

function tick() {
  for (const s of sensors) {
    // random walk: small step up/down, kept inside min..max
    const step = (Math.random() - 0.5) * (s.max - s.min) * 0.02;
    s.value = Math.min(s.max, Math.max(s.min, s.value + step));

    const message = {
      sensor_id: s.sensor_id,
      station: s.station,
      zone: s.zone,
      metric: s.metric,
      value: Number(s.value.toFixed(2)),
      unit: s.unit,
      protocol: s.protocol,
      battery: Number((3.0 + Math.random() * 0.6).toFixed(2)), // volts
      rssi: -50 - Math.floor(Math.random() * 40),              // dBm
      timestamp: new Date().toISOString(),
    };

    // Topic design: twin/<station>/<zone>/<sensor_id>/telemetry
    const topic = `twin/${s.station}/${s.zone}/${s.sensor_id}/telemetry`;
    client.publish(topic, JSON.stringify(message), { qos: 1 });
    console.log("->", topic, message.value, message.unit);
  }
}
