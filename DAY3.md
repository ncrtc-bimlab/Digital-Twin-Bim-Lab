# Day 3: Kafka in the middle

```
fake sensor -> MQTT -> bridge -> KAFKA (telemetry.raw) -+-> consume:db    -> TimescaleDB
                                       |                +-> consume:cache -> Redis
                                       +-> telemetry.rejected (bad messages)
```

`ingest.js` (Day 2) is replaced by `bridge` + two consumers. Do not run `ingest` anymore.

## Steps

1. Add three lines to the END of your `.env` (do not replace the file):
   ```bash
   cat >> .env <<'EOF'
   KAFKA_BROKERS=localhost:9094
   COMPOSE_FILE=docker-compose.yml:docker-compose.kafka.yml
   EOF
   ```
   (`COMPOSE_FILE` makes plain `docker compose` read both compose files.)

2. Start everything (first time downloads Kafka and the UI, a few minutes):
   ```bash
   docker compose up -d
   docker compose ps          # twin-kafka and twin-kafka-ui must be "running"
   ```
   If a port is taken (9094 or 8089), change the LEFT number in `docker-compose.kafka.yml`
   (and `KAFKA_BROKERS` for 9094; in `KAFKA_ADVERTISED_LISTENERS` the EXTERNAL port too).

3. `npm install`
4. `npm run db:setup`  (adds a "no duplicate readings" rule).
   If it complains about duplicates: `docker exec twin-db psql -U twin -d twin -c "TRUNCATE readings;"` and run it again.
5. `npm run kafka:setup`  (creates the topics)
6. Start each in its own terminal:
   ```bash
   npm run bridge
   npm run consume:db
   npm run consume:cache
   npm run publish
   ```
7. `npm run status`  (same as before)
8. Open http://localhost:8089  ->  Topics -> telemetry.raw -> Messages.
   Also: Consumers -> twin-timescale and twin-redis-cache.

## Experiments (this is where the learning is)

A. **Consumer down, no loss.** Ctrl+C `consume:db` for a minute. In Kafka UI, watch the
   group's *lag* grow. Start it again: lag falls to 0 and `npm run status` shows no gap.

B. **Database down, no loss** (the test that failed on Day 2).
   `docker stop twin-db`, wait 30 s, `docker start twin-db`. The consumer retries and catches up.

C. **Scale out.** Start a second `npm run consume:db` in a new terminal. In Kafka UI the group now
   has 2 members and the 6 partitions are split between them. With 7 consumers, one sits idle.
   Partitions = the limit on parallel consumers.

D. **Replay.** Stop `consume:cache`, then:
   ```bash
   docker exec twin-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
     --group twin-redis-cache --reset-offsets --to-earliest --topic telemetry.raw --execute
   ```
   Start it again: Redis is rebuilt from the log. Redis is now disposable.

E. **Bad data.** Send junk:
   ```bash
   docker exec twin-mqtt mosquitto_pub -t twin/ANAV/P123/XX1/telemetry -m '{"foo":1}'
   ```
   The bridge rejects it and it appears in `telemetry.rejected` (look in Kafka UI). Nothing crashes.

## Concepts
- **Topic**: a named log. Messages are appended and kept for 7 days (our setting), not deleted when read.
- **Partition**: a topic is split into parallel lanes. Same key -> same partition -> order preserved.
- **Key**: we use `sensor_id`, so each sensor's readings stay in order.
- **Offset**: position in a partition. A consumer group remembers "I have processed up to here".
- **Consumer group**: a team sharing the work. Different groups each get ALL messages.
- **Lag**: how far a group is behind the newest message. The key health number for pipelines.
- **At-least-once**: a message can arrive twice; storage is built to ignore the second copy.
- **Ack only after durable**: the bridge acknowledges MQTT only after Kafka confirmed.

## Homework
1. Run `npm run publish` in two terminals again. Does the DB row count still double? (Think about the unique rule and timestamps.)
2. Stop `bridge` for a minute while publishing. What happens to readings? (Hint: MQTT durable session.)
3. Look at which partition each sensor lands in (Kafka UI -> Messages -> filter by partition).
