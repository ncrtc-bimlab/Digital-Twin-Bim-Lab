// Ingest service: MQTT -> validate -> TimescaleDB (history) + Redis (latest).
// Day 5 or so, Kafka will sit between the MQTT and this logic. The logic stays the same.

const mqtt = require("mqtt");
const { Pool } = require("pg");
const Redis = require("ioredis");

const MQTT_URL = process.env.MQTT_URL || "mqtt://localhost:1883";
const DATABASE_URL =
  process.env.DATABASE_URL || "postgres://twin:twin_dev_pw@localhost:5432/twin";
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

const FLUSH_EVERY_MS = 1000; // write to the database at most once per second
const FLUSH_AT_ROWS = 200;   // or as soon as this many rows are waiting

const REQUIRED = ["sensor_id", "metric", "value", "timestamp"];

const pool = new Pool({ connectionString: DATABASE_URL });
// Without this handler, Node CRASHES when Postgres drops an idle connection.
pool.on("error", (e) => console.error("DB connection lost (will reconnect):", e.message));

const redis = new Redis(REDIS_URL);
redis.on("error", (e) => console.error("Redis error:", e.message));

// Fixed clientId + clean:false = a "durable session". While this service is down,
// the broker keeps the QoS 1 messages and delivers them when we come back.
// (Only ONE ingest process can use this clientId at a time.)
const client = mqtt.connect(MQTT_URL, {
  clientId: "twin-ingest-1",
  clean: false,
});

const MAX_BUFFER = 50000; // safety cap so a long DB outage can't eat all memory
let flushing = false;

let buffer = [];
let stats = { received: 0, rejected: 0, stored: 0 };

function validate(msg) {
  for (const k of REQUIRED) if (msg[k] === undefined) return `missing ${k}`;
  if (typeof msg.value !== "number" || !Number.isFinite(msg.value)) return "value is not a number";
  const t = Date.parse(msg.timestamp);
  if (Number.isNaN(t)) return "bad timestamp";
  // Reject readings from the far future (a classic broken-clock problem on devices)
  if (t > Date.now() + 5 * 60 * 1000) return "timestamp in the future";
  return null;
}

async function updateLatest(msg) {
  const key = `latest:${msg.sensor_id}`;
  await redis
    .multi()
    .hset(key, {
      metric: msg.metric,
      value: msg.value,
      unit: msg.unit ?? "",
      station: msg.station ?? "",
      zone: msg.zone ?? "",
      protocol: msg.protocol ?? "",
      battery: msg.battery ?? "",
      rssi: msg.rssi ?? "",
      time: msg.timestamp,
    })
    // Sorted set of "last time we heard from each sensor". Powers the "gone quiet" check.
    .zadd("lastseen", Date.now(), msg.sensor_id)
    .exec();
}

async function flush() {
  if (flushing || buffer.length === 0) return;
  flushing = true;
  const rows = buffer;
  buffer = [];
  try {
    // One INSERT for the whole batch (much faster than one per row)
    await pool.query(
      `INSERT INTO readings (time, sensor_id, station, zone, metric, value, unit, protocol, battery, rssi)
       SELECT * FROM unnest(
         $1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[],
         $6::float8[], $7::text[], $8::text[], $9::real[], $10::int[]
       )`,
      [
        rows.map((r) => r.timestamp),
        rows.map((r) => r.sensor_id),
        rows.map((r) => r.station ?? null),
        rows.map((r) => r.zone ?? null),
        rows.map((r) => r.metric),
        rows.map((r) => r.value),
        rows.map((r) => r.unit ?? null),
        rows.map((r) => r.protocol ?? null),
        rows.map((r) => r.battery ?? null),
        rows.map((r) => r.rssi ?? null),
      ]
    );
    stats.stored += rows.length;
  } catch (e) {
    console.error("DB write failed, will retry:", e.message);
    buffer = rows.concat(buffer); // put them back so nothing is lost
    if (buffer.length > MAX_BUFFER) {
      const dropped = buffer.length - MAX_BUFFER;
      buffer = buffer.slice(dropped); // drop the oldest, keep the newest
      console.error(`Buffer full: dropped ${dropped} oldest readings`);
    }
  } finally {
    flushing = false;
  }
}

client.on("connect", () => {
  console.log("MQTT connected");
  client.subscribe("twin/+/+/+/telemetry", { qos: 1 });
});

client.on("message", async (topic, payload) => {
  stats.received++;
  let msg;
  try {
    msg = JSON.parse(payload.toString());
  } catch {
    stats.rejected++;
    return console.error("BAD JSON on", topic);
  }
  const problem = validate(msg);
  if (problem) {
    stats.rejected++;
    return console.error(`REJECTED (${problem}) on ${topic}`);
  }
  buffer.push(msg);
  updateLatest(msg).catch((e) => console.error("Redis failed:", e.message));
  if (buffer.length >= FLUSH_AT_ROWS) flush();
});

setInterval(flush, FLUSH_EVERY_MS);
setInterval(() => console.log("stats", stats), 10000);

process.on("SIGINT", async () => {
  console.log("Shutting down, flushing...");
  await flush();
  await pool.end();
  redis.disconnect();
  client.end();
  process.exit(0);
});