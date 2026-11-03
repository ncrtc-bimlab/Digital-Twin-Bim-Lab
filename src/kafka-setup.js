// Creates the Kafka topics. Run once: npm run kafka:setup  (safe to repeat)
const { kafka, TOPICS } = require("./kafka");

const SEVEN_DAYS_MS = String(7 * 24 * 60 * 60 * 1000);

(async () => {
  const admin = kafka.admin();
  await admin.connect();

  const created = await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic: TOPICS.RAW,
        // Partitions = the maximum number of consumers that can work in parallel.
        // You can add partitions later but never remove them, so leave headroom.
        numPartitions: 6,
        replicationFactor: 1, // 1 because we have one dev broker. Production uses 3.
        configEntries: [{ name: "retention.ms", value: SEVEN_DAYS_MS }],
      },
      {
        topic: TOPICS.REJECTED,
        numPartitions: 1,
        replicationFactor: 1,
        configEntries: [{ name: "retention.ms", value: SEVEN_DAYS_MS }],
      },
    ],
  });
  console.log(created ? "Topics created." : "Topics already existed.");

  const meta = await admin.fetchTopicMetadata({ topics: [TOPICS.RAW, TOPICS.REJECTED] });
  for (const t of meta.topics) console.log(`  ${t.name}: ${t.partitions.length} partition(s)`);

  await admin.disconnect();
})().catch((e) => {
  console.error("Kafka setup failed:", e.message);
  console.error("Is Kafka running?  docker compose ps   (and KAFKA_BROKERS in .env)");
  process.exit(1);
});
