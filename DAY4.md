# Day 4: Connect ChirpStack (real LoRaWAN data)

```
LoRa sensors -> gateway -> ChirpStack -> its MQTT -> [adapter-chirpstack] -> our MQTT
                                                       -> bridge -> Kafka -> DB + Redis
```

The adapter only READS from ChirpStack. It never writes to ChirpStack or to your gateway.

## Part A: practice with the simulator (no hardware, safe)

1. Add to the END of `.env` (do not replace the file):
   ```bash
   cat >> .env <<'EOF'
   CHIRPSTACK_MQTT_URL=mqtt://localhost:1883
   CHIRPSTACK_MQTT_USER=
   CHIRPSTACK_MQTT_PASS=
   EOF
   ```
   For now ChirpStack's "broker" is our own local one, which is where the simulator writes.

2. Run the tests (they need nothing installed): `npm test`  -> 8 passing.
3. Keep running: `bridge`, `consume:db`, `consume:cache`. Stop the Day 1 `publish` if you like.
4. New terminals:
   ```bash
   npm run adapter:chirpstack
   npm run sim:chirpstack
   ```
5. After 30 seconds: `npm run status`. You should see:
   - `LW001` in `ANAV/P123` with temperature AND humidity (one sensor, two measurements)
   - `node-unknown` in `UNMAPPED/UNMAPPED` (device not in `config/devices.json`)
   - `node-raw` decoded by our own decoder (30 C, 60 %)
   The adapter prints one warning per problem device, not one per message.

6. Edit `config/devices.json` while the adapter runs (map `a1b2c3d4e5f60718` to a real-looking sensor id).
   It reloads in 2 seconds, no restart. New readings use the new name.

## Part B: connect your real ChirpStack

### B1. Collect facts (in the ChirpStack web page)
- Version (shown on the login page or in the footer). This code supports **v4**. If you have v3, tell me.
- Open one device -> **Events** tab. Click an "up" event. Does the JSON contain an `object` with readings (decoded)?
  If `object` is missing, the device profile has no codec (see "Decoders" below).
- Note the Application ID and one DevEUI.

### B2. Find ChirpStack's MQTT broker
It is whatever ChirpStack's MQTT integration points to:
- Installed with Docker: look in its `docker-compose.yml` for a `mosquitto` service, and in `chirpstack.toml` / `region_*.toml` under `[integration.mqtt]` (`server = "tcp://host:1883"`, `username`, `password`).
- Note the host, port and credentials if any.

### B3. Peek without writing anything
`mosquitto_sub` is already inside our broker container, so you do not need to install anything:
```bash
docker exec twin-mqtt mosquitto_sub -h <CHIRPSTACK_HOST> -p 1883 \
  -t 'application/+/device/+/event/up' -C 3 -v
# add   -u USER -P PASS   if it needs credentials
```
It prints 3 uplinks and exits. If it just waits, no device has sent data yet, or the host/port/credentials
are wrong. **Paste me one uplink with any secrets removed** and I will check the adapter against your exact format.

### B4. Point the adapter at it
In `.env`:
```
CHIRPSTACK_MQTT_URL=mqtt://<host>:1883
CHIRPSTACK_MQTT_USER=<user or leave empty>
CHIRPSTACK_MQTT_PASS=<password or leave empty>
```
Add your devices to `config/devices.json`, then `npm run adapter:chirpstack`.
The simulator keeps writing only to the local broker, even after you change this.

### Safety rules
- Use a dedicated MQTT account for us with **read-only** access to `application/#` if you can.
- Never write passwords into code. They stay in `.env`, which is never committed.
- Keep keys (AppKey, NwkKey) out of everything we build. We never need them.

## Decoders
Radio bytes must become numbers. Two ways:
1. **In ChirpStack** (recommended): device profile -> Codec -> JavaScript. ChirpStack then sends `object`.
2. **In this project**: create `decoders/<device profile name>.js` exporting a function `(bytes, fPort) => ({temperature: ...})`.
   See `decoders/temp-humidity.js`. One small file per sensor MODEL, not per sensor, is how this scales to thousands.

## What changed
- `consumer-cache.js` and `status.js`: Redis now stores one entry per measurement, so a sensor with temperature AND humidity is not overwritten.
  Old Redis data is harmless. To rebuild it cleanly in the new shape, use the replay trick from Day 3 experiment D.
- `package.json`: new scripts `adapter:chirpstack`, `sim:chirpstack`, `test`.

## Concepts
- **Adapter pattern**: every protocol (LoRaWAN, BLE, WiFi) gets a small adapter that produces our standard message. Nothing downstream knows or cares where it came from.
- **Device register**: DevEUI is the radio identity; `sensor_id` is YOUR identity from the equipment register. The mapping connects the physical world to your digital twin. For thousands of sensors this moves from a JSON file into the database (next step).
- **Unmapped is a state, not an error**: unknown devices still flow, flagged, so nothing silently disappears.
- **Sanitising topics**: names from outside are cleaned before they become MQTT topics.
- **Pure functions are testable**: `chirpstack-transform.js` has no network code, so 8 tests run in milliseconds.

## Homework
1. Add a fourth device to the simulator with `{ pm25: 35, pm10: 60 }` and check it appears with the right units.
2. Stop the adapter for a minute. What happens to uplinks meanwhile? (ChirpStack publishes at QoS 0, so think about what that means.)
3. Write a test for a case I did not cover (for example an uplink with an empty `object` and no data).
