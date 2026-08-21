import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '../generated/prisma/client';
import 'dotenv/config';

const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL
});
const prisma = new PrismaClient({ adapter });

async function main() {
  console.log('Seeding 10,000 orders...');
  
  const batchSize = 1000;
  const totalRecords = 10000;
  
  for (let i = 0; i < totalRecords; i += batchSize) {
    const data = Array.from({ length: batchSize }).map(() => ({
      userId: Math.floor(Math.random() * 1000) + 1,
      amount: Math.floor(Math.random() * 50000) + 100, // random amount
      status: Math.random() > 0.5 ? 'completed' : 'pending',
    }));
    
    await prisma.order.createMany({
      data,
    });
    console.log(`Inserted batch ${i / batchSize + 1} / ${totalRecords / batchSize}`);
  }
  
  console.log('Seeding finished successfully.');
}

main()
  .catch((e) => {
    console.error(e);
    process.exit(1);
  })
  .finally(async () => {
    await prisma.$disconnect();
  });
