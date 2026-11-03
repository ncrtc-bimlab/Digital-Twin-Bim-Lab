// Kafka -> TimescaleDB consumer (consumer group: twin-timescale).
// Offsets are committed only AFTER the rows are in the database.
// If the database is down, we do not move forward; Kafka keeps the data and we resume later.

const { Pool } = require("pg");
const { kafka, TOPICS } = require("./kafka");

const DATABASE_URL =
  process.env.DATABASE_URL || "postgres://twin:twin_dev_pw@localhost:5433/twin";

const pool = new Pool({ connectionString: DATABASE_URL });
pool.on("error", (e) => console.error("DB connection lost (will reconnect):", e.message));

async function insertRows(rows) {
  // ON CONFLICT DO NOTHING = if Kafka delivers the same reading twice
  // (it can, "at least once"), the second copy is silently ignored.
  await pool.query(
    `INSERT INTO readings (time, sensor_id, station, zone, metric, value, unit, protocol, battery, rssi)
     SELECT * FROM unnest(
       $1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[],
       $6::float8[], $7::text[], $8::text[], $9::real[], $10::int[]
     )
     ON CONFLICT (sensor_id, metric, time) DO NOTHING`,
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
}

(async () => {
  const consumer = kafka.consumer({ groupId: "twin-timescale" });
  await consumer.connect();
  // fromBeginning only matters the FIRST time this group exists
  await consumer.subscribe({ topic: TOPICS.RAW, fromBeginning: true });

  let stored = 0;
  setInterval(() => console.log("consumer-db: rows handled =", stored), 10000);

  await consumer.run({
    eachBatchAutoResolve: false, // we decide when an offset counts as "done"
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      const rows = [];
      for (const m of batch.messages) {
        try {
          rows.push(JSON.parse(m.value.toString()));
        } catch {
          /* the bridge already validated; skip anything unreadable */
        }
      }

      // If this throws, Kafka retries the same batch later: nothing is lost.
      if (rows.length > 0) await insertRows(rows);
      stored += rows.length;

      resolveOffset(batch.messages[batch.messages.length - 1].offset);
      await heartbeat();
    },
  });

  consumer.on(consumer.events.CRASH, (e) =>
    console.error("Consumer crashed, restarting:", e.payload.error.message)
  );

  console.log("consumer-db running (group twin-timescale)");

  process.on("SIGINT", async () => {
    await consumer.disconnect();
    await pool.end();
    process.exit(0);
  });
})().catch((e) => {
  console.error("consumer-db failed to start:", e.message);
  process.exit(1);
});
