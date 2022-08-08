const { Kafka } = require("kafkajs");

createConsumer();

async function createConsumer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client1",
      brokers: ["172.29.48.1:9092"]
    });

    const consumer = kafka.consumer({
      groupId: "log_store_consumer_group5"
    });

    console.log("Consumer'a bağlanılıyor..");
    await consumer.connect();
    console.log("Bağlantı başarılı.");

    // Consumer Subscribe..
    await consumer.subscribe({
      topic: "Emiralp",
      fromBeginning: true
    });

    await consumer.run({
      eachMessage: async result => {
        console.log(
          `Gelen Mesaj ${result.message.value}, Par => ${result.partition}`
        );
      }
    });
  } catch (error) {
    console.log("Bir Hata Oluştu", error);
  }
}