// ChirpStack adapter.
// Listens to ChirpStack's MQTT integration (READ ONLY), converts each uplink into our
// standard message, and publishes it to OUR broker. From there the bridge, Kafka and the
// consumers take over, exactly as for the fake sensors.
//
//   ChirpStack MQTT --> [this adapter] --> our MQTT --> bridge --> Kafka --> DB + Redis

const fs = require("fs");
const path = require("path");
const mqtt = require("mqtt");
const { toMessages } = require("./chirpstack-transform");

const CS_URL = process.env.CHIRPSTACK_MQTT_URL || "mqtt://localhost:1883";
const CS_USER = process.env.CHIRPSTACK_MQTT_USER || undefined;
const CS_PASS = process.env.CHIRPSTACK_MQTT_PASS || undefined;
const OUT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";

// ChirpStack v4 topic: application/<application id>/device/<DevEUI>/event/up
const UPLINK_TOPIC = "application/+/device/+/event/up";

// ---- device register (DevEUI -> our sensor id / station / zone) ----
const DEVICES_FILE = path.join(__dirname, "..", "config", "devices.json");
let devices = {};
function loadDevices() {
  try {
    const raw = JSON.parse(fs.readFileSync(DEVICES_FILE, "utf8"));
    const next = {};
    for (const [eui, v] of Object.entries(raw)) {
      if (!eui.startsWith("_")) next[eui.toLowerCase()] = v; // keys starting with _ are comments
    }
    devices = next;
    console.log(`Loaded ${Object.keys(devices).length} device(s) from config/devices.json`);
  } catch (e) {
    console.error("Could not read config/devices.json:", e.message, "(keeping previous list)");
  }
}
loadDevices();
fs.watchFile(DEVICES_FILE, { interval: 2000 }, loadDevices); // edit the file, no restart needed

// ---- optional per-model decoders: decoders/<device profile name>.js ----
const decoderCache = new Map();
function getDecoder(profile) {
  if (!profile) return null;
  const name = String(profile).replace(/[^A-Za-z0-9_-]/g, "_");
  if (!decoderCache.has(name)) {
    let fn = null;
    try {
      fn = require(path.join(__dirname, "..", "decoders", `${name}.js`));
    } catch {
      /* no decoder for this profile */
    }
    decoderCache.set(name, typeof fn === "function" ? fn : null);
  }
  return decoderCache.get(name);
}

// ---- connections ----
const out = mqtt.connect(OUT_URL);
out.on("error", (e) => console.error("Our MQTT error:", e.message));

const cs = mqtt.connect(CS_URL, { username: CS_USER, password: CS_PASS });
cs.on("error", (e) => console.error("ChirpStack MQTT error:", e.message));

const stats = { uplinks: 0, published: 0, bad_json: 0, undecoded: 0, unmapped: 0, other: 0 };
const warned = new Set(); // warn only once per problem + device, not on every uplink

cs.on("connect", () => {
  console.log(`Connected to ChirpStack MQTT at ${CS_URL}`);
  cs.subscribe(UPLINK_TOPIC, { qos: 0 }, (err) => {
    if (err) console.error("Subscribe failed:", err.message);
    else console.log(`Listening on ${UPLINK_TOPIC}`);
  });
});

cs.on("message", (topic, payload) => {
  stats.uplinks++;
  let ev;
  try {
    ev = JSON.parse(payload.toString());
  } catch {
    stats.bad_json++;
    return;
  }

  const { out: messages, issue } = toMessages(ev, devices, getDecoder);

  if (issue) {
    if (issue.code in stats) stats[issue.code]++;
    else stats.other++;
    const key = `${issue.code}:${issue.devEui}`;
    if (!warned.has(key)) {
      warned.add(key);
      if (issue.code === "undecoded") {
        console.warn(
          `[undecoded] ${issue.devEui} profile=${issue.profile} data=${issue.data}\n` +
            `   -> add a codec to this device profile in ChirpStack, or create decoders/${issue.profile}.js`
        );
      } else if (issue.code === "unmapped") {
        console.warn(
          `[unmapped] ${issue.devEui}: published as sensor "${issue.sensor_id}" in UNMAPPED. Add it to config/devices.json`
        );
      } else {
        console.warn(`[${issue.code}] ${issue.devEui}`);
      }
    }
  }

  for (const m of messages) {
    out.publish(m.topic, JSON.stringify(m.message), { qos: 1 });
    stats.published++;
  }
});

setInterval(() => console.log("adapter", stats), 10000);

process.on("SIGINT", () => {
  cs.end();
  out.end();
  process.exit(0);
});
