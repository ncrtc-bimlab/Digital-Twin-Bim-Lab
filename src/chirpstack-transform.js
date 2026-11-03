// Converts ONE ChirpStack v4 uplink event into our standard messages.
// Pure functions only (no network, no files), so it is easy to test.
//
// A ChirpStack uplink looks roughly like:
//   { time, deviceInfo:{devEui, deviceName, deviceProfileName,...}, fPort, data (base64),
//     object (already decoded by ChirpStack's codec), rxInfo:[{gatewayId, rssi, snr}] }

// Names a sensor may use for the same thing -> our metric name + unit
const KNOWN = {
  temperature: ["temperature", "C"],
  temp: ["temperature", "C"],
  humidity: ["humidity", "%"],
  hum: ["humidity", "%"],
  co2: ["co2", "ppm"],
  pressure: ["pressure", "hPa"],
  pm25: ["pm2_5", "ug/m3"],
  pm10: ["pm10", "ug/m3"],
  voc: ["voc", "ppb"],
  noise: ["noise", "dB"],
  light: ["light", "lux"],
};

// These describe the device, not the environment, so they go into the "battery" field
const BATTERY_KEYS = new Set(["battery", "batt", "vbat", "battery_v", "batteryvoltage"]);

// Anything that ends up inside an MQTT topic must not contain / # + or spaces
const clean = (s) => String(s).replace(/[^A-Za-z0-9_-]/g, "_");

function toMessages(ev, devices = {}, getDecoder = () => null) {
  const devEui = String(ev?.deviceInfo?.devEui ?? ev?.devEUI ?? "").toLowerCase();
  if (!devEui) return { out: [], issue: { code: "no_deveui", devEui: "" } };

  // 1) Prefer what ChirpStack already decoded
  let obj =
    ev.object && typeof ev.object === "object" && Object.keys(ev.object).length > 0
      ? ev.object
      : null;

  // 2) Otherwise try our own decoder for this device profile (decoders/<profile>.js)
  if (!obj && ev.data) {
    const decoder = getDecoder(ev.deviceInfo?.deviceProfileName);
    if (decoder) {
      try {
        obj = decoder(Buffer.from(ev.data, "base64"), ev.fPort) || null;
      } catch {
        obj = null;
      }
    }
  }
  if (!obj) {
    return {
      out: [],
      issue: { code: "undecoded", devEui, profile: ev.deviceInfo?.deviceProfileName, data: ev.data },
    };
  }

  const dev = devices[devEui];
  const sensor_id = clean(dev?.sensor_id ?? ev.deviceInfo?.deviceName ?? devEui);
  const station = clean(dev?.station ?? "UNMAPPED");
  const zone = clean(dev?.zone ?? "UNMAPPED");

  // ChirpStack lists every gateway that heard the frame. Keep the strongest one.
  const best = [...(ev.rxInfo || [])].sort((a, b) => (b.rssi ?? -999) - (a.rssi ?? -999))[0];

  const timestamp =
    ev.time && !Number.isNaN(Date.parse(ev.time))
      ? new Date(ev.time).toISOString()
      : new Date().toISOString();

  let battery;
  const metrics = [];
  for (const [rawKey, rawVal] of Object.entries(obj)) {
    const key = rawKey.toLowerCase();
    let value = rawVal;
    if (typeof value === "boolean") value = value ? 1 : 0; // door open/closed -> 1/0
    if (typeof value !== "number" || !Number.isFinite(value)) continue; // skip text/objects
    if (BATTERY_KEYS.has(key)) {
      battery = value;
      continue;
    }
    const [metric, unit] = KNOWN[key] ?? [clean(key), ""];
    metrics.push({ metric, unit, value });
  }
  if (metrics.length === 0) return { out: [], issue: { code: "no_metrics", devEui } };

  const topic = `twin/${station}/${zone}/${sensor_id}/telemetry`;
  const out = metrics.map((m) => ({
    topic,
    message: {
      sensor_id,
      station,
      zone,
      metric: m.metric,
      value: m.value,
      unit: m.unit,
      protocol: "lorawan",
      battery,
      rssi: best?.rssi,
      snr: best?.snr,
      gateway_id: best?.gatewayId,
      dev_eui: devEui,
      timestamp,
    },
  }));

  return { out, issue: dev ? null : { code: "unmapped", devEui, sensor_id } };
}

module.exports = { toMessages, clean };
