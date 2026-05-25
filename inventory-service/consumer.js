const { consumer } = require("../kafka/client");
const { startElection, amILeader, getCurrentLeader } = require("../coordinator/leader");
const { redis, connectRedis, getOrSet } = require("../redis/redis");

let processingEnabled = false;

async function start() {
    // Remove: processingEnabled = true;  ← DELETE THIS LINE

    // Start ZooKeeper election first
    await startElection();

    // Wait a bit for election to complete
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Only start consumer if we're leader
    if (amILeader()) {
        console.log("✅ Starting Kafka consumer as LEADER");
        await startKafkaConsumer();
    } else {
        console.log(`⏸️ Waiting as FOLLOWER. Current leader: ${getCurrentLeader()}`);
        monitorLeadership();
    }
}

async function startKafkaConsumer() {
    console.log("🔍 DEBUG: startKafkaConsumer() called");
    console.log("🔍 DEBUG: Setting processingEnabled = true");

    processingEnabled = true;

    console.log("🔍 DEBUG: processingEnabled is now:", processingEnabled);
    console.log("🔍 DEBUG: About to connect consumer...");

    await consumer.connect();
    console.log("🔍 DEBUG: Consumer connected successfully");

    await consumer.subscribe({ topic: "order.created" });
    console.log("🔍 DEBUG: Subscribed to order.created");

    console.log("🔍 DEBUG: About to start consumer.run()...");

    await consumer.run({
        eachMessage: async ({ message, topic, partition, heartbeat }) => {
            console.log("🔍 DEBUG: eachMessage() called - WE GOT A MESSAGE!");

            if (!amILeader()) {
                console.log("❌ Lost leadership, stopping processing");
                processingEnabled = false;
                return;
            }

            if (!processingEnabled) {
                console.log("Processing disabled, skipping");
                return;
            }

            try {
                const order = JSON.parse(message.value.toString());
                console.log(`📦 Processing order: ${order.orderId}, quantity: ${order.quantity}`);

                // FIX 1: Define stockKey and stock properly
                const stockKey = `product:${order.productId}`;
                await connectRedis();

                const stock = await getOrSet(
                    stockKey,
                    async () => {
                        return 100; // ONLY first time initialization
                    }
                );
                console.log(`🔍 DEBUG: Current stock for ${stockKey} = ${stock}`);

                if (isNaN(stock)) {
                    console.log(`❌ Product ${order.productId} not found in Redis`);
                    return;
                }


                const newStock = await redis.decrBy(stockKey, order.quantity);

                if (newStock < 0) {
                    await redis.incrBy(stockKey, order.quantity); // rollback
                    console.log("❌ Out of stock");
                } else {
                    console.log(`✅ Reserved stock. Remaining: ${newStock}`);
                }
            } catch (error) {
                console.error("Error processing message:", error);
            }

            await heartbeat();
        },
    });
}

function monitorLeadership() {
    setInterval(() => {
        if (amILeader() && !processingEnabled) {
            console.log("🔄 Became leader, restarting consumer...");
            startKafkaConsumer().catch(console.error);
        } else if (!amILeader() && processingEnabled) {
            console.log("🔄 Lost leadership, stopping consumer...");
            processingEnabled = false;
            consumer.disconnect().catch(console.error);
        }
    }, 3000);
}

process.on('SIGINT', async () => {
    console.log("\n👋 Shutting down...");
    processingEnabled = false;
    await consumer.disconnect();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log("\n👋 Shutting down...");
    processingEnabled = false;
    await consumer.disconnect();
    process.exit(0);
});

start().catch(console.error);