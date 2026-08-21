import autocannon from 'autocannon';

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

const STATUSES = ['pending', 'completed', 'cancelled'];

autocannon({
  url: 'http://localhost:3000/order',
  connections: 10,
  duration: 30,
  pipelining: 1,
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  setupClient: (client) => {
    client.setBody(
      JSON.stringify({
        userId: randomInt(1, 1000),
        amount: randomInt(100, 100000),
        status: STATUSES[randomInt(0, STATUSES.length - 1)],
      })
    );
  },
}, (err, result) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(autocannon.printResult(result));
});