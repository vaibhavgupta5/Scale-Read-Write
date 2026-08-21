import Fastify from "fastify";
import { PrismaPg } from "@prisma/adapter-pg";
import "dotenv/config";
import { PrismaClient } from "../generated/prisma/client";
import Redis from 'ioredis';

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

app.get("/order", async () => {
  const id = Math.floor(Math.random() * ORDER_ID_MAX) + 1;
  const cacheKey = `order:${id}`;

  try {
    const cachedOrder = await redis.get(cacheKey);
    if (cachedOrder) {
      return JSON.parse(cachedOrder);
    }
  } catch (error) {
    console.warn(`Redis GET error for key ${cacheKey}:`, error);
  }

  const order = await prisma.order.findFirst({
    where: {
      id,
    },
  });

  if (order) {
    try {
      await redis.setex(cacheKey, 60, JSON.stringify(order));
    } catch (error) {
      console.warn(`Redis SETEX error for key ${cacheKey}:`, error);
    }
  }

  return order;
});

app.post("/order", async (req: any, res: any) => {
  const { userId, amount, status } = req.body;
  const payload = {
    userId: parseInt(userId),
    amount: parseInt(amount),
    ...(status && { status }),
  };

  try {
    await redis.rpush('orderQueue', JSON.stringify(payload));
  } catch (error) {
    console.warn('Redis queue push error, falling back to direct write:', error);
    const order = await prisma.order.create({ data: payload });
    return order;
  }

  return { status: "queued" };
});

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
  console.log('Shutting down... flushing queue.');
  clearInterval(flusherInterval);
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
