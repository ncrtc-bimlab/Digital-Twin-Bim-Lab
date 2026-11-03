// EXAMPLE decoder. The byte format here is INVENTED for practice:
//   byte 0-1: temperature, signed 16-bit, hundredths of a degree C
//   byte 2  : humidity, half-percent steps
// A real sensor's format is in its datasheet. Name the file after the ChirpStack
// DEVICE PROFILE name (decoders/<profile name>.js) and the adapter will use it
// whenever ChirpStack did not decode the frame itself.

module.exports = function decode(bytes /* Buffer */, fPort) {
  if (bytes.length < 3) return null;
  const raw = (bytes[0] << 8) | bytes[1];
  const temperature = (raw & 0x8000 ? raw - 0x10000 : raw) / 100;
  const humidity = bytes[2] / 2;
  return { temperature, humidity };
};
