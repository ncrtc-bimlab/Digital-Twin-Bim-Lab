// Creates the tables. Run once: npm run db:setup
const fs = require("fs");
const path = require("path");
const { Client } = require("pg");

const DATABASE_URL =
  process.env.DATABASE_URL || "postgres://twin:twin_dev_pw@localhost:5433/twin";

(async () => {
  const client = new Client({ connectionString: DATABASE_URL });
  await client.connect();
  const sql = fs.readFileSync(path.join(__dirname, "..", "db", "schema.sql"), "utf8");
  await client.query(sql);
  console.log("Schema ready.");
  await client.end();
})().catch((e) => {
  console.error("Setup failed:", e.message);
  process.exit(1);
});
