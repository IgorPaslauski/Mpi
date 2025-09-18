// master.js
import 'dotenv/config';
import express from 'express';
import { fork } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import amqplib from 'amqplib';
import { Pool } from 'pg';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Config
const PORT = Number(process.env.PORT || 3000);
const SLAVES = Number(process.env.SLAVES || 3);
const SUM_TIMEOUT_MS = Number(process.env.SUM_TIMEOUT_MS || 1500);
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const PGSSL = String(process.env.PGSSL || 'false').toLowerCase() === 'true';
const pgPool = new Pool({
    host: process.env.PGHOST,
    port: Number(process.env.PGPORT || 5432),
    database: process.env.PGDATABASE,
    user: process.env.PGUSER,
    password: process.env.PGPASSWORD,
    ssl: PGSSL ? { rejectUnauthorized: false } : false
});

// RabbitMQ setup
const R_KEYS = {
    WRITES_QUEUE: 'writes.q',
    SUM_EXCHANGE: 'sum.fanout'
};

let amqpConn, amqpCh;

function spawnSlavesLocal() {
    for (let i = 1; i <= SLAVES; i++) {
        const p = fork(path.join(__dirname, 'worker.js'), [], {
            env: { SLAVE_ID: String(i) } // cada worker terá seu id
        });
        p.on('exit', (code) => console.error(`[master] slave ${i} saiu (code=${code})`));
    }
}

async function setupRabbit() {
    amqpConn = await amqplib.connect(RABBITMQ_URL);
    amqpConn.on('error', (e) => console.error('[AMQP] connection error:', e?.message || e));
    amqpConn.on('close', () => console.error('[AMQP] connection closed'));

    amqpCh = await amqpConn.createChannel();
    amqpCh.on('error', (e) => console.error('[AMQP] channel error:', e?.message || e));

    await amqpCh.assertQueue(R_KEYS.WRITES_QUEUE, { durable: true, autoDelete: false });

    await amqpCh.assertExchange(R_KEYS.SUM_EXCHANGE, 'fanout', { durable: false });

    console.log('[AMQP] ready');
}

async function publishWrite(value) {
    const payload = Buffer.from(JSON.stringify({ value }));
    const ok = amqpCh.sendToQueue(R_KEYS.WRITES_QUEUE, payload, {
        contentType: 'application/json',
        deliveryMode: 2
    });
    if (!ok) throw new Error('Falha ao publicar write');
}

async function requestSumAggregate(timeoutMs = SUM_TIMEOUT_MS) {
    const { queue: replyQueue } = await amqpCh.assertQueue('', {
        exclusive: true,
        autoDelete: true
    });

    const correlationId = `sum-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    let total = 0;
    let received = 0;

    const onMsg = (msg) => {
        if (!msg) return;
        if (msg.properties.correlationId !== correlationId) return;
        try {
            const body = JSON.parse(msg.content.toString('utf8'));
            if (typeof body.sum === 'number') {
                total += body.sum;
                received++;
            }
        } catch (_) { }
    };

    const { consumerTag } = await amqpCh.consume(replyQueue, onMsg, { noAck: true });

    amqpCh.publish(
        R_KEYS.SUM_EXCHANGE,
        '',
        Buffer.from(JSON.stringify({ type: 'sum' })),
        {
            contentType: 'application/json',
            correlationId,
            replyTo: replyQueue
        }
    );

    await new Promise((r) => setTimeout(r, timeoutMs));
    await amqpCh.cancel(consumerTag);

    return { total, answered: received, timeoutMs };
}

async function main() {
    await setupRabbit();

    const app = express();
    app.use(express.json());
    app.use('/', express.static(path.join(__dirname, 'public')));

    app.post('/api/write', async (req, res) => {
        const value = Number(req.body?.value);
        if (!Number.isInteger(value)) return res.status(400).json({ ok: false, error: 'value deve ser inteiro' });
        try {
            await publishWrite(value);
            return res.json({ ok: true, value });
        } catch (e) {
            return res.status(500).json({ ok: false, error: String(e?.message || e) });
        }
    });

    app.get('/api/sum', async (_req, res) => {
        try {
            const { total, answered, timeoutMs } = await requestSumAggregate(SUM_TIMEOUT_MS);
            return res.json({ ok: true, total, answered, timeoutMs });
        } catch (e) {
            return res.status(500).json({ ok: false, error: String(e?.message || e) });
        }
    });

    app.get('/api/stats', async (_req, res) => {
        try {
            const { rows } = await pgPool.query(
                'SELECT slave_id, COUNT(*)::int AS items FROM numbers GROUP BY slave_id ORDER BY slave_id'
            );
            return res.json({ ok: true, slaves: rows });
        } catch (e) {
            return res.status(500).json({ ok: false, error: String(e?.message || e) });
        }
    });

    app.listen(PORT, () => {
        console.log(`[master] HTTP em http://localhost:${PORT}`);
    });

    spawnSlavesLocal();
}

main().catch((err) => {
    console.error('Falha ao iniciar master:', err);
    process.exit(1);
});
