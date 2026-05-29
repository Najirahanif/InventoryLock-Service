const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "inventory-lock",
  //for single broker
  // brokers: ["10.55.66.131:9092"], // Use your broker IP
  // for mulltiple broker
  brokers: [
    "10.55.66.131:9092",
    "10.55.66.131:9093",
    "10.55.66.131:9094"
  ],
  ssl: false, // since SASL_PLAINTEXT
  sasl: {
    mechanism: "plain", // Change from scram-sha-256 to plain
    username: "najira", // Use the user you defined in JAAS config
    password: "naji12", // Use the password from JAAS config
  },
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "inventory-group" });

module.exports = { kafka, producer, consumer };