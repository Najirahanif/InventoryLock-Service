const { producer } = require("../kafka/client");
const redis = require("../redis/redis");

async function start() {
  await producer.connect();
  // await redis.set("product:p1", 5);

  setInterval(async () => {
    const order = {
      orderId: Date.now(),
      productId: "p1",
      quantity: 3,
    };

    await producer.send({
      topic: "order.created",
      messages: [
        {
          value: JSON.stringify(order),
        },
      ],
    });

    console.log("Order sent:", order);
  }, 3000);
}

start();