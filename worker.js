// worker.js
import 'dotenv/config';
import amqplib from 'amqplib';
import { Pool } from 'pg';

const SLAVE_ID = Number(process.env.SLAVE_ID || 0);
if (!SLAVE_ID) {
    console.error('[worker] SLAVE_ID não definido');
    process.exit(1);
}

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const R_KEYS = { WRITES_QUEUE: 'writes.q', SUM_EXCHANGE: 'sum.fanout' };

const PGSSL = String(process.env.PGSSL || 'false').toLowerCase() === 'true';
const pgPool = new Pool({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: PGSSL ? { rejectUnauthorized: false } : false
});

async function ensureSchema() {
    await pgPool.query(`
    CREATE TABLE IF NOT EXISTS numbers (
      id         bigserial PRIMARY KEY,
      slave_id   int NOT NULL,
      value      int NOT NULL,
      created_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_numbers_slave ON numbers (slave_id);
  `);
}

async function start() {
    await ensureSchema();

    const conn = await amqplib.connect(RABBITMQ_URL);
    const ch = await conn.createChannel();

    await ch.assertQueue(R_KEYS.WRITES_QUEUE, { durable: true, autoDelete: false });
    ch.prefetch(1);
    ch.consume(R_KEYS.WRITES_QUEUE, async (msg) => {
        if (!msg) return;
        try {
            const body = JSON.parse(msg.content.toString('utf8'));
            const v = Number(body.value);
            if (Number.isInteger(v)) {
                await pgPool.query('INSERT INTO numbers(slave_id, value) VALUES ($1, $2)', [SLAVE_ID, v]);
            }
            ch.ack(msg);
        } catch (e) {
            console.error(`[worker ${SLAVE_ID}] erro write:`, e);
            ch.nack(msg, false, false);
        }
    });

    await ch.assertExchange(R_KEYS.SUM_EXCHANGE, 'fanout', { durable: false });
    const { queue } = await ch.assertQueue('', { exclusive: true, autoDelete: true });
    await ch.bindQueue(queue, R_KEYS.SUM_EXCHANGE, '');

    ch.consume(queue, async (msg) => {
        if (!msg) return;
        try {
            const { rows } = await pgPool.query('SELECT COALESCE(SUM(value),0)::bigint AS sum FROM numbers WHERE slave_id = $1', [SLAVE_ID]);
            const sum = Number(rows?.[0]?.sum || 0);

            const replyTo = msg.properties.replyTo;
            const correlationId = msg.properties.correlationId;
            if (replyTo && correlationId) {
                ch.sendToQueue(replyTo, Buffer.from(JSON.stringify({ slaveId: SLAVE_ID, sum })), {
                    contentType: 'application/json',
                    correlationId
                });
            }
        } catch (e) {
            console.error(`[worker ${SLAVE_ID}] erro sum:`, e);
        } finally {
            ch.ack(msg);
        }
    });

    console.log(`[worker ${SLAVE_ID}] pronto.`);
}

start().catch((err) => {
    console.error(`[worker ${SLAVE_ID}] falha ao iniciar:`, err);
    process.exit(1);
});
