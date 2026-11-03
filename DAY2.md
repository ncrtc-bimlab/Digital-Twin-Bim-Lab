# Day 2: Store the readings

Goal: `fake sensor -> MQTT -> ingest service -> TimescaleDB (history) + Redis (latest)`

## New files
- `db/schema.sql`: the `readings` table (a TimescaleDB hypertable)
- `src/db-setup.js`: runs the schema
- `src/ingest.js`: the service that listens, validates, and stores
- `src/status.js`: shows live state and history

## Steps

1. Services still running? (`docker compose ps`). If not: `docker compose up -d`
2. Install the new libraries: `npm install`
3. Create the tables: `npm run db:setup` (should print `Schema ready.`)
4. Terminal A: `npm run ingest`
5. Terminal B: `npm run publish`
6. Wait a minute, then in Terminal C: `npm run status`

You should see each sensor's latest value, "ok" status, and a growing count of stored readings.

## Test the "gone quiet" idea
1. Stop the publisher (Ctrl+C in Terminal B).
2. Wait 40 seconds, then run `npm run status`.
3. All sensors now show `QUIET`. This is the same logic as the "86 gone quiet" number in your dashboard.

## Look inside the database yourself
```bash
docker exec -it twin-db psql -U twin -d twin
```
```sql
SELECT * FROM readings ORDER BY time DESC LIMIT 5;

-- average temperature per minute for one sensor
SELECT time_bucket('1 minute', time) AS minute, avg(value)
FROM readings WHERE sensor_id = 'AV070'
GROUP BY minute ORDER BY minute DESC LIMIT 10;
```
Exit with `\q`.

## Look inside Redis
```bash
docker exec -it twin-redis redis-cli
HGETALL latest:AV070
ZRANGE lastseen 0 -1 WITHSCORES
```

## Concepts
- **Hypertable**: a normal-looking table that TimescaleDB splits by time behind the scenes.
- **Batching**: we write up to 200 rows in one INSERT (or once per second). Row-by-row inserts are the most common beginner performance mistake.
- **Redis = latest, Postgres = history.** The live screen reads Redis (fast). Charts read TimescaleDB.
- **Validation at the door**: bad JSON, missing fields, and future timestamps are rejected and counted.
- **No lost data on DB failure**: if a write fails, rows go back into the buffer and are retried.

## Homework
- Run two publishers at once (two terminals). Does the count double?
- Stop the database (`docker stop twin-db`) for 20 seconds, then start it again. Watch the ingest logs. Is data lost?
- Write a SQL query for the max temperature in the last hour.
