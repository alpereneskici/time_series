const { Kafka } = require("kafkajs");

const log_data = require("./data_file.json")

createProducer();

async function createProducer() {
  try {
    const kafka = new Kafka({
      clientId: "kafka_log_store_client2",
      brokers: ["172.29.48.1:9092"]
    });

    const producer = kafka.producer();
    console.log("Producer'a bağlanılıyor..");
    await producer.connect();
    console.log("Bağlantı başarılı.");
    let messages = log_data.map(item =>{
      return {
        value: JSON.stringify(item),
        partition: item.type =="Sattelite-1,pump 151/1" ? 0 : 1
      };
    })
    const message_result = await producer.send({
      topic: "Emiralp",
      messages: messages
    });
    console.log("Gonderim işlemi başarılıdır", JSON.stringify(message_result));
    await producer.disconnect();
  } catch (error) {
    console.log("Bir Hata Oluştu", error);
  } finally {
    process.exit(0);
  }
}