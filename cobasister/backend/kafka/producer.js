const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'donasi-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

const sendDonasiEvent = async (data) => {
  await producer.connect()
  await producer.send({
    topic: 'donasi-topic',
    messages: [{ value: JSON.stringify(data) }]
  })
  await producer.disconnect()
}

module.exports = sendDonasiEvent
