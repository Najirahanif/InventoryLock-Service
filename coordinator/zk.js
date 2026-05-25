const zookeeper = require("node-zookeeper-client");

// Connect to your 3-node ensemble
const ZOOKEEPER_HOSTS = process.env.ZOOKEEPER_HOSTS || "localhost:2181";
// For 3-node cluster:
// const ZOOKEEPER_HOSTS = "zk1:2181,zk2:2181,zk3:2181";

const client = zookeeper.createClient(ZOOKEEPER_HOSTS, {
    sessionTimeout: 30000,
    spinDelay: 1000,
    retries: 5
});

// Connection events
client.once('connected', () => {
    console.log(`✅ Connected to ZooKeeper: ${ZOOKEEPER_HOSTS}`);
});

client.on('disconnected', () => {
    console.log("⚠️ Disconnected from ZooKeeper");
});

client.on('expired', () => {
    console.log("❌ Session expired");
    // Reconnect logic
    setTimeout(() => {
        client.connect();
    }, 5000);
});

client.connect();

module.exports = client;