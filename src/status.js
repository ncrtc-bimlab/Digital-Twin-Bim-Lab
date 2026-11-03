// Quick health view: what's live (Redis) and what's stored (TimescaleDB).
// Run: npm run status          (show more rows: LIMIT=200 npm run status)

const Redis = require("ioredis");
const { Pool } = require("pg");

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6380";
const DATABASE_URL =
  process.env.DATABASE_URL || "postgres://twin:twin_dev_pw@localhost:5433/twin";
const QUIET_AFTER_MS = Number(process.env.QUIET_AFTER_MS || 30000);
const LIMIT = Number(process.env.LIMIT || 40);

(async () => {
  const redis = new Redis(REDIS_URL);
  const pool = new Pool({ connectionString: DATABASE_URL });

  // 1) Latest value of every sensor (from Redis, instant)
  const ids = await redis.zrange("lastseen", 0, -1);
  console.log(`\n== LIVE (Redis): ${ids.length} sensors ==`);
  const now = Date.now();
  let quiet = 0;

  for (const [i, id] of ids.entries()) {
    const lastSeen = Number(await redis.zscore("lastseen", id));
    const ageMs = now - lastSeen;
    const isQuiet = ageMs > QUIET_AFTER_MS;
    if (isQuiet) quiet++;
    if (i >= LIMIT) continue; // still counted above, just not printed

    const v = await redis.hgetall(`latest:${id}`);
    const metrics = Object.entries(v)
      .filter(([k]) => k.startsWith("m:"))
      .map(([k, json]) => {
        const m = JSON.parse(json);
        return `${k.slice(2)}=${m.value}${m.unit}`;
      });
    if (metrics.length === 0 && v.metric) metrics.push(`${v.metric}=${v.value}${v.unit}`); // old format

    console.log(
      `${id.padEnd(16)} ${(isQuiet ? "QUIET" : "ok").padEnd(6)} ${String((ageMs / 1000).toFixed(0)).padStart(5)}s ago` +
        `  ${(v.station || "?")}/${(v.zone || "?")}  ${metrics.join("  ")}`
    );
  }
  if (ids.length > LIMIT) console.log(`... and ${ids.length - LIMIT} more (LIMIT=${ids.length} npm run status)`);
  console.log(`quiet: ${quiet} of ${ids.length}`);

  // 2) History (from TimescaleDB)
  const total = await pool.query("SELECT count(*) FROM readings");
  console.log(`\n== HISTORY (TimescaleDB): ${total.rows[0].count} readings stored ==`);

  const last = await pool.query(
    `SELECT sensor_id, metric, round(avg(value)::numeric, 2) AS avg, min(value) AS min, max(value) AS max, count(*) AS n
       FROM readings
      WHERE time > now() - interval '10 minutes'
      GROUP BY sensor_id, metric ORDER BY sensor_id, metric LIMIT 60`
  );
  console.log("\nLast 10 minutes per sensor and metric:");
  console.table(last.rows);

  await pool.end();
  redis.disconnect();
})().catch((e) => {
  console.error("Status failed:", e.message);
  process.exit(1);
});
