const fastify = require("fastify")();
const { Kafka, logLevel } = require("kafkajs");

const kafka = new Kafka({
  clientId: "api",
  brokers: ["localhost:9092"],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 10
  }
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "test" });

fastify.get("/", async (request, reply) => {
  return { path: "root" };
});

fastify.get("/message", async (request, reply) => {
  await producer.connect();
  await producer.send({
    topic: "test-message",
    messages: [{ value: "Hello KafkaJS user!" }]
  });

  await producer.disconnect();
  return { path: "message" };
});

const start = async () => {
  try {
    await producer.connect();
    await consumer.connect();

    await consumer.subscribe({ topic: "test-response" });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(String(message.value));
        console.log(topic);
      }
    });

    await fastify.listen(5000);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
