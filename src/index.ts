import Fastify from "fastify";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool } from 'pg';
import "dotenv/config";
import { PrismaClient } from "../generated/prisma/client";
import Redis from 'ioredis';
import fastJson from 'fast-json-stringify';
import { LRUCache } from './lru-cache';

const stringifyOrderPayload = fastJson({
  type: 'object',
  properties: {
    userId: { type: 'integer' },
    amount: { type: 'integer' },
    status: { type: 'string' }
  },
  required: ['userId', 'amount']
});

const stringifyOrder = fastJson({
  type: 'object',
  properties: {
    id: { type: 'integer' },
    userId: { type: 'integer' },
    amount: { type: 'integer' },
    status: { type: 'string' }
  }
});

// Connection pool optimization
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: parseInt(process.env.DB_POOL_MAX || '20'),
  idleTimeoutMillis: parseInt(process.env.DB_POOL_IDLE_TIMEOUT || '30000'),
  connectionTimeoutMillis: parseInt(process.env.DB_POOL_TIMEOUT || '5000'),
});
const adapter = new PrismaPg(pool);
const prisma = new PrismaClient({ adapter });
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: 3,
  enableReadyCheck: true,
  lazyConnect: false,
});

// Fastify performance tuning
const app = Fastify({
  logger: false,
  ignoreTrailingSlash: true,
  onProtoPoisoning: 'remove',
  requestIdLogLabel: 'reqId',
  disableRequestLogging: true,
  ajv: {
    customOptions: {
      removeAdditional: 'all',
      coerceTypes: false,
      useDefaults: true,
    }
  }
});

app.get("/health", async () => {
  await prisma.$queryRaw`SELECT 1`;
  return { ok: true };
});

const ORDER_ID_MAX = Number(process.env.ORDER_ID_MAX?.replace(/_/g, '') ?? 10_000);
const L1_CACHE_SIZE = parseInt(process.env.L1_CACHE_SIZE || '10000');
const L1_CACHE_TTL = parseInt(process.env.L1_CACHE_TTL || '60000');

// LRU cache with size limit
const l1Cache = new LRUCache<string, { data: any, expiresAt: number }>(L1_CACHE_SIZE);

// Stats tracking (disabled by default in production)
const ENABLE_STATS = process.env.ENABLE_STATS === 'true';
let stats = { l1: 0, l2: 0, db: 0 };
if (ENABLE_STATS) {
  setInterval(() => {
    console.log(`Cache stats - L1: ${stats.l1}, L2: ${stats.l2}, DB (miss): ${stats.db}`);
  }, 5000);
}

app.get("/order", {
  schema: {
    response: {
      200: {
        type: 'object',
        properties: {
          id: { type: 'integer' },
          userId: { type: 'integer' },
          amount: { type: 'integer' },
          status: { type: 'string', nullable: true }
        }
      }
    }
  }
}, async () => {
  // Optimized random ID generation
  const id = ((Math.random() * ORDER_ID_MAX) | 0) + 1;
  const cacheKey = `order:${id}`;
  const now = Date.now();

  // 1. L1 Cache (In-Memory)
  const l1Hit = l1Cache.get(cacheKey);
  if (l1Hit && l1Hit.expiresAt > now) {
    if (ENABLE_STATS) stats.l1++;
    return l1Hit.data;
  }

  // 2. L2 Cache (Redis)
  try {
    const cachedOrder = await redis.get(cacheKey);
    if (cachedOrder) {
      if (ENABLE_STATS) stats.l2++;
      const parsed = JSON.parse(cachedOrder);
      l1Cache.set(cacheKey, { data: parsed, expiresAt: now + L1_CACHE_TTL });
      return parsed;
    }
  } catch (error) {
    console.warn(`Redis GET error for key ${cacheKey}:`, error);
  }

  // 3. Database
  if (ENABLE_STATS) stats.db++;
  const order = await prisma.order.findUnique({
    where: {
      id,
    },
  });

  if (order) {
    l1Cache.set(cacheKey, { data: order, expiresAt: now + L1_CACHE_TTL });
    // Fire-and-forget cache write (non-blocking)
    redis.setex(cacheKey, 60, stringifyOrder(order)).catch(error => {
      console.warn(`Redis SETEX error for key ${cacheKey}:`, error);
    });
  }

  return order;
});

const localWriteBuffer: string[] = [];

app.post("/order", {
  schema: {
    body: {
      type: 'object',
      properties: {
        userId: { type: 'integer' },
        amount: { type: 'integer' },
        status: { type: 'string', nullable: true }
      },
      required: ['userId', 'amount']
    },
    response: {
      200: {
        type: 'object',
        properties: {
          status: { type: 'string' }
        }
      }
    }
  }
}, async (req: any, res: any) => {
  const { userId, amount, status } = req.body;
  const payload = {
    userId: parseInt(userId),
    amount: parseInt(amount),
    ...(status && { status }),
  };

  localWriteBuffer.push(stringifyOrderPayload(payload));
  return { status: "queued" };
});

const REDIS_FLUSH_INTERVAL = parseInt(process.env.REDIS_FLUSH_INTERVAL || '200');

const flushToRedis = async () => {
  if (localWriteBuffer.length === 0) return;
  const batch = localWriteBuffer.splice(0, localWriteBuffer.length);
  try {
    await redis.rpush('orderQueue', ...batch);
  } catch (error) {
    console.warn('Redis queue push error, falling back to direct write:', error);
    const objects = batch.map(b => JSON.parse(b));
    await prisma.order.createMany({ data: objects }).catch(e => console.error('Fallback DB write failed', e));
  }
};

const redisFlusherInterval = setInterval(flushToRedis, REDIS_FLUSH_INTERVAL);

const DB_BATCH_SIZE = parseInt(process.env.DB_BATCH_SIZE || '1000');

const flushQueue = async () => {
  try {
    const pipeline = redis.multi();
    pipeline.lrange('orderQueue', 0, -1);
    pipeline.del('orderQueue');

    const results = await pipeline.exec();
    if (!results || results.length === 0) return;

    const firstResult = results[0];
    if (!firstResult || firstResult[0]) return;

    const itemsStr = firstResult[1] as string[];
    if (!itemsStr || itemsStr.length === 0) return;

    const batch = itemsStr.map(item => JSON.parse(item));

    // Chunk large batches to prevent query timeouts
    for (let i = 0; i < batch.length; i += DB_BATCH_SIZE) {
      const chunk = batch.slice(i, i + DB_BATCH_SIZE);
      await prisma.order.createMany({
        data: chunk,
      });
    }

    console.log(`Flushed ${batch.length} orders to DB.`);
  } catch (error) {
    console.error('Error flushing order queue:', error);
  }
};

const flusherInterval = setInterval(flushQueue, 5000);

const shutdown = async () => {
  console.log('Shutting down... flushing queues.');
  clearInterval(redisFlusherInterval);
  clearInterval(flusherInterval);
  await flushToRedis();
  await flushQueue();
  await prisma.$disconnect();
  redis.disconnect();
  process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

app.listen({ port: 3000 }, async (err) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }

  // Connection warmup
  try {
    await prisma.$queryRaw`SELECT 1`;
    console.log('✓ Database connection warmed up');
  } catch (error) {
    console.error('Database warmup failed:', error);
  }

  try {
    await redis.ping();
    console.log('✓ Redis connection warmed up');
  } catch (error) {
    console.error('Redis warmup failed:', error);
  }

  console.log("listening on 3000");
});
