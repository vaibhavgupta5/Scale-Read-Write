import Fastify from "fastify";
import { PrismaPg } from "@prisma/adapter-pg";
import "dotenv/config";
import { PrismaClient } from "../generated/prisma/client";

const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL,
});
const prisma = new PrismaClient({ adapter });
const app = Fastify({ logger: false });

app.get("/health", async () => {
  await prisma.$queryRaw`SELECT 1`;
  return { ok: true };
});

const ORDER_ID_MAX = Number(process.env.ORDER_ID_MAX?.replace(/_/g, '') ?? 10_000);

app.get("/order", async () => {
  const id = Math.floor(Math.random() * ORDER_ID_MAX) + 1;
  const order = await prisma.order.findFirst({
    where: {
      id,
    },
  });
  return order;
});

app.post("/order", async (req: any, res: any) => {
  const { userId, amount, status } = req.body;
  const order = await prisma.order.create({
    data: {
      userId: parseInt(userId),
      amount: parseInt(amount),
      ...(status && { status }),
    },
  });
  return order;
});

app.listen({ port: 3000 }, (err) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log("listening on 3000");
});
