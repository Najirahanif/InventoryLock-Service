const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "inventory-lock",
  brokers: ["192.168.1.23:9092"], // Use your broker IP
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