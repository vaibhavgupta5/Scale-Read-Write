import Fastify from "fastify";
import { PrismaPg } from "@prisma/adapter-pg";
import "dotenv/config";
import { PrismaClient } from "../generated/prisma/client";
import Redis from 'ioredis';
import fastJson from 'fast-json-stringify';

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

const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL,
});
const prisma = new PrismaClient({ adapter });
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const app = Fastify({ logger: false });

app.get("/health", async () => {
  await prisma.$queryRaw`SELECT 1`;
  return { ok: true };
});

const ORDER_ID_MAX = Number(process.env.ORDER_ID_MAX?.replace(/_/g, '') ?? 10_000);
const l1Cache = new Map<string, { data: any, expiresAt: number }>();

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
  const id = Math.floor(Math.random() * ORDER_ID_MAX) + 1;
  const cacheKey = `order:${id}`;
  const now = Date.now();

  // 1. L1 Cache (In-Memory)
  const l1Hit = l1Cache.get(cacheKey);
  if (l1Hit && l1Hit.expiresAt > now) {
    return l1Hit.data;
  }

  // 2. L2 Cache (Redis)
  try {
    const cachedOrder = await redis.get(cacheKey);
    if (cachedOrder) {
      const parsed = JSON.parse(cachedOrder);
      l1Cache.set(cacheKey, { data: parsed, expiresAt: now + 60000 });
      return parsed;
    }
  } catch (error) {
    console.warn(`Redis GET error for key ${cacheKey}:`, error);
  }

  // 3. Database
  const order = await prisma.order.findFirst({
    where: {
      id,
    },
  });

  if (order) {
    l1Cache.set(cacheKey, { data: order, expiresAt: now + 60000 });
    try {
      await redis.setex(cacheKey, 60, stringifyOrder(order));
    } catch (error) {
      console.warn(`Redis SETEX error for key ${cacheKey}:`, error);
    }
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

const redisFlusherInterval = setInterval(flushToRedis, 200);

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
    
    await prisma.order.createMany({
      data: batch,
    });
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

app.listen({ port: 3000 }, (err) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log("listening on 3000");
});
