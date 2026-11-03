// Kafka -> Redis consumer (consumer group: twin-redis-cache).
// A SEPARATE group from the database consumer, so it gets its own full copy of the stream
// and progresses at its own speed. If Redis is lost, reset this group's offset and replay.
//
// Redis layout (changed on Day 4 so one sensor can carry several measurements):
//   HASH  latest:<sensor_id>   m:temperature = {"value":..,"unit":..,"time":..}
//                              m:humidity    = {...}
//                              station, zone, protocol, battery, rssi, gateway_id, last_time
//   ZSET  lastseen             sensor_id -> time the bridge received its last reading

const Redis = require("ioredis");
const { kafka, TOPICS } = require("./kafka");

const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6380";

const redis = new Redis(REDIS_URL);
redis.on("error", (e) => console.error("Redis error:", e.message));

(async () => {
  const consumer = kafka.consumer({ groupId: "twin-redis-cache" });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.RAW, fromBeginning: true });

  let handled = 0;
  setInterval(() => console.log("consumer-cache: readings handled =", handled), 10000);

  await consumer.run({
    eachBatchAutoResolve: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      const pipe = redis.pipeline(); // many commands, one network round trip
      let n = 0;

      for (const m of batch.messages) {
        let msg;
        try {
          msg = JSON.parse(m.value.toString());
        } catch {
          continue;
        }
        pipe.hset(`latest:${msg.sensor_id}`, {
          [`m:${msg.metric}`]: JSON.stringify({
            value: msg.value,
            unit: msg.unit ?? "",
            time: msg.timestamp,
          }),
          station: msg.station ?? "",
          zone: msg.zone ?? "",
          protocol: msg.protocol ?? "",
          battery: msg.battery ?? "",
          rssi: msg.rssi ?? "",
          gateway_id: msg.gateway_id ?? "",
          last_time: msg.timestamp,
        });
        // "Last heard" uses the time the bridge received it (Kafka's own timestamp),
        // not the device clock, and GT means it can only move forward, so replaying
        // old data can never make a sensor look like it was heard in the past.
        pipe.zadd("lastseen", "GT", Number(m.timestamp), String(msg.sensor_id));
        n++;
      }

      if (n > 0) {
        const results = await pipe.exec();
        const failed = results.find(([err]) => err);
        if (failed) throw failed[0]; // Kafka will retry the batch
        handled += n;
      }

      resolveOffset(batch.messages[batch.messages.length - 1].offset);
      await heartbeat();
    },
  });

  console.log("consumer-cache running (group twin-redis-cache)");

  process.on("SIGINT", async () => {
    await consumer.disconnect();
    redis.disconnect();
    process.exit(0);
  });
})().catch((e) => {
  console.error("consumer-cache failed to start:", e.message);
  process.exit(1);
});
