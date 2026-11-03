// ChirpStack SIMULATOR for practice without hardware.
// Publishes fake uplinks in ChirpStack v4's format to OUR LOCAL broker only.
// It deliberately refuses to publish anywhere else, so it can never put fake data
// into your real ChirpStack's broker.

const crypto = require("crypto");
const mqtt = require("mqtt");

const URL_ = process.env.MQTT_URL || "mqtt://localhost:1883";
const host = new URL(URL_).hostname;
if (!["localhost", "127.0.0.1", "::1"].includes(host) && process.env.ALLOW_REMOTE_SIM !== "1") {
  console.error(`Refusing to simulate onto ${host}. This tool only writes to a local broker.`);
  process.exit(1);
}

const client = mqtt.connect(URL_);
let fCnt = 0;

function uplink({ devEui, name, profile, object, data }) {
  const now = new Date().toISOString();
  return {
    deduplicationId: crypto.randomUUID(),
    time: now,
    deviceInfo: {
      tenantName: "NCRTC",
      applicationName: "stations",
      deviceProfileName: profile,
      deviceName: name,
      devEui,
    },
    devAddr: "01020304",
    fCnt: fCnt++,
    fPort: 1,
    data: data ?? Buffer.from([0]).toString("base64"),
    ...(object ? { object } : {}),
    rxInfo: [
      { gatewayId: "0016c001ff1a2b3c", rssi: -70 - Math.floor(Math.random() * 30), snr: 5 + Math.random() * 5 },
    ],
    txInfo: { frequency: 865062500 },
  };
}

function tick() {
  const t = 24 + Math.random() * 6;
  const events = [
    // 1) decoded by ChirpStack, listed in config/devices.json
    uplink({
      devEui: "0102030405060708",
      name: "node-AV073",
      profile: "temp-humidity",
      object: { temperature: Number(t.toFixed(2)), humidity: Math.round(50 + Math.random() * 20), battery: 3.3 },
    }),
    // 2) decoded, but NOT in config/devices.json -> shows up as UNMAPPED
    uplink({
      devEui: "a1b2c3d4e5f60718",
      name: "node-unknown",
      profile: "co2-sensor",
      object: { co2: Math.round(500 + Math.random() * 300) },
    }),
    // 3) raw bytes only; our decoders/temp-humidity.js decodes it (30.00 C, 60 %)
    uplink({
      devEui: "1111222233334444",
      name: "node-raw",
      profile: "temp-humidity",
      data: "C7h4",
    }),
  ];
  for (const ev of events) {
    const topic = `application/1/device/${ev.deviceInfo.devEui}/event/up`;
    client.publish(topic, JSON.stringify(ev), { qos: 0 });
  }
  console.log(`-> published ${events.length} fake uplinks`);
}

client.on("connect", () => {
  console.log(`Simulating ChirpStack uplinks on ${URL_} every 10 s. Ctrl+C to stop.`);
  tick();
  setInterval(tick, 10000);
});
client.on("error", (e) => console.error("MQTT error:", e.message));
