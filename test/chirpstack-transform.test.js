// Test with: npm test    (uses Node's built-in test runner, nothing to install)
const test = require("node:test");
const assert = require("node:assert");
const { toMessages } = require("../src/chirpstack-transform");
const decodeExample = require("../decoders/temp-humidity");

const devices = { "0102030405060708": { sensor_id: "LW001", station: "ANAV", zone: "P123" } };

const baseEvent = (over = {}) => ({
  time: "2026-11-03T10:00:00Z",
  deviceInfo: { devEui: "0102030405060708", deviceName: "node-1", deviceProfileName: "temp-humidity" },
  fPort: 1,
  rxInfo: [
    { gatewayId: "gw-weak", rssi: -110, snr: 1 },
    { gatewayId: "gw-strong", rssi: -80, snr: 8.5 },
  ],
  ...over,
});

test("decoded uplink becomes one message per metric, using the strongest gateway", () => {
  const { out, issue } = toMessages(
    baseEvent({ object: { temperature: 27.5, humidity: 61, battery: 3.4 } }),
    devices
  );
  assert.strictEqual(issue, null);
  assert.strictEqual(out.length, 2);
  assert.strictEqual(out[0].topic, "twin/ANAV/P123/LW001/telemetry");
  assert.strictEqual(out[0].message.metric, "temperature");
  assert.strictEqual(out[0].message.unit, "C");
  assert.strictEqual(out[0].message.battery, 3.4);
  assert.strictEqual(out[0].message.rssi, -80);
  assert.strictEqual(out[0].message.gateway_id, "gw-strong");
  assert.strictEqual(out[0].message.protocol, "lorawan");
});

test("battery is not reported as an environmental metric", () => {
  const { out } = toMessages(baseEvent({ object: { temperature: 20, battery: 3.3 } }), devices);
  assert.deepStrictEqual(out.map((o) => o.message.metric), ["temperature"]);
});

test("unknown device is still processed but flagged as unmapped", () => {
  const ev = baseEvent({ object: { temperature: 20 } });
  ev.deviceInfo.devEui = "aaaaaaaaaaaaaaaa";
  const { out, issue } = toMessages(ev, devices);
  assert.strictEqual(issue.code, "unmapped");
  assert.strictEqual(out[0].message.sensor_id, "node-1"); // falls back to ChirpStack device name
  assert.strictEqual(out[0].message.station, "UNMAPPED");
});

test("raw bytes with no decoder are reported as undecoded", () => {
  const { out, issue } = toMessages(baseEvent({ data: "C7h4" }), devices, () => null);
  assert.strictEqual(out.length, 0);
  assert.strictEqual(issue.code, "undecoded");
});

test("our own decoder is used when ChirpStack did not decode", () => {
  // bytes 0x0B 0xB8 0x78 -> 3000/100 = 30.00 C, 120/2 = 60 %
  const { out } = toMessages(baseEvent({ data: "C7h4" }), devices, () => decodeExample);
  assert.strictEqual(out.length, 2);
  assert.strictEqual(out.find((o) => o.message.metric === "temperature").message.value, 30);
  assert.strictEqual(out.find((o) => o.message.metric === "humidity").message.value, 60);
});

test("negative temperatures decode correctly", () => {
  // -5.00 C = -500 = 0xFE0C
  const r = decodeExample(Buffer.from([0xfe, 0x0c, 0x50]), 1);
  assert.strictEqual(r.temperature, -5);
});

test("booleans become 1/0 and text values are skipped", () => {
  const { out } = toMessages(baseEvent({ object: { door_open: true, status: "ok", temp: 21 } }), devices);
  const byMetric = Object.fromEntries(out.map((o) => [o.message.metric, o.message.value]));
  assert.deepStrictEqual(byMetric, { door_open: 1, temperature: 21 });
});

test("dangerous characters never reach the MQTT topic", () => {
  const ev = baseEvent({ object: { temperature: 20 } });
  ev.deviceInfo.devEui = "bbbbbbbbbbbbbbbb";
  ev.deviceInfo.deviceName = "evil/#+ name";
  const { out } = toMessages(ev, {});
  assert.ok(!/[#+ ]/.test(out[0].topic.replace(/\//g, "")));
  assert.strictEqual(out[0].topic.split("/").length, 5);
});
