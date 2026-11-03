// One place that decides what a "valid reading" is.
// Returns null if OK, or a short reason string if the message must be rejected.

const REQUIRED = ["sensor_id", "metric", "value", "timestamp"];

function validate(msg) {
  for (const k of REQUIRED) if (msg[k] === undefined) return `missing ${k}`;
  if (typeof msg.value !== "number" || !Number.isFinite(msg.value)) return "value is not a number";
  const t = Date.parse(msg.timestamp);
  if (Number.isNaN(t)) return "bad timestamp";
  // Far-future timestamps are a classic broken-device-clock symptom
  if (t > Date.now() + 5 * 60 * 1000) return "timestamp in the future";
  return null;
}

module.exports = { validate };
