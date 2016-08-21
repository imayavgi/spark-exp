import java.util
import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConversions._

/**
  * Created by imaya on 7/31/16.
  */
object KafkaMsgConsumer {
  def main(args: Array[String]): Unit = {
    val example = new ScalaConsumerExample("localhost:9092", "imaya-cg", "imaya-topic", 1)
    example.run(2)
  }
}

class ScalaConsumerExample(val brokerList: String,
                           val groupId: String,
                           val topic: String,
                           val delay: Long) extends Logging {

  val consumer = new KafkaConsumer[String, String](createConsumerConfig(brokerList, groupId))
  val topics = new util.ArrayList[String]()
  topics.add(topic)
  consumer.subscribe(topics)

  def createConsumerConfig(broker: String, groupId: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", broker)
    props.put("group.id", groupId)
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run(numThreads : Int): Unit = {
    while (true) {
      val records = consumer.poll(100)
      records.foreach(record => println(s"offset = ${record.offset()} key = ${record.key()}, value = ${record.value()}"))
    }
  }

}
