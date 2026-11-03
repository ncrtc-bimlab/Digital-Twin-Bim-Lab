# NCRTC Digital Twin: Day 1

Goal: fake sensor -> MQTT broker -> subscriber, with TimescaleDB and Redis running ready for later.

## Folder layout
```
ncrtc-twin/
  docker-compose.yml        # MQTT broker + TimescaleDB + Redis
  mosquitto/config/mosquitto.conf
  package.json
  src/publisher.js          # fake sensors
  src/subscriber.js         # listener + validator
```

## Steps

1. Check tools (Node 20+ is needed):
   ```bash
   docker --version
   node --version
   ```
   If Node is missing, install it from https://nodejs.org (LTS).

2. Start the services:
   ```bash
   docker compose up -d
   docker compose ps        # all 3 should say "running"
   ```

3. Install the MQTT library:
   ```bash
   npm install
   ```

4. Terminal A (listener):
   ```bash
   npm run subscribe
   ```

5. Terminal B (fake sensors):
   ```bash
   npm run publish
   ```
   Terminal A should now print a reading every 5 seconds per sensor.

## Check the other services
```bash
docker exec -it twin-redis redis-cli ping                 # PONG
docker exec -it twin-db psql -U twin -d twin -c "select version();"
```

## Understand it (Day 1 homework)
- Change `INTERVAL_MS`: `INTERVAL_MS=1000 npm run publish`
- Add a 5th sensor in `publisher.js`.
- Subscribe to only one station: change the topic in `subscriber.js` to `twin/ANAV/P123/+/telemetry`.
- Delete a field from the message and see the subscriber reject it.

## Concepts learned
- **Broker**: the post office. Publishers send to topics, subscribers receive.
- **Topic**: `twin/<station>/<zone>/<sensor_id>/telemetry`. `+` matches one level, `#` matches all below.
- **QoS 1**: the message is delivered at least once.
- **Standard message**: every protocol (LoRaWAN, BLE, WiFi) will be converted into this same JSON shape.

## Stop / reset
```bash
docker compose down          # stop, keep data
docker compose down -v       # stop and DELETE all data
```
# nitin-test-dt
# nitin-test-dt
