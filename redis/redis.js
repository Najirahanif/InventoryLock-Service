const { createClient } = require("redis");

const redis = createClient();

redis.on("error", (err) => console.error("Redis Error:", err));

const connectRedis = async () => {
  if (!redis.isOpen) {
    await redis.connect();
    console.log("Redis connected");
  }
};

async function getOrSet(key, defaultValueFn, ttl = null) {
  const cached = await redis.get(key);

  if (cached !== null) {
    return JSON.parse(cached);
  }

  const value = await defaultValueFn();

  await redis.set(
    key,
    JSON.stringify(value),
    ttl ? { EX: ttl } : undefined
  );

  return value;
}

module.exports = {
  redis,
  connectRedis,
  getOrSet,
};