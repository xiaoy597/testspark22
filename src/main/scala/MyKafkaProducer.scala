/**
  * Created by xiaoy on 2017/11/27.
  */

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object MyKafkaProducer {
  def main(args: Array[String]) {

    val topic = args(0)
    val dataFile = args(1)

    val props = new Properties()
    props.put("bootstrap.servers", "host2:6667")
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    for (batch <- 0 to 5) {
      println("Batch %d begins.".format(batch))

      val source = Source.fromFile(dataFile)
      var count = 0
      for (line <- source.getLines()) {
        val fields = line.split("\\|")
        // 深市过户表
        if (topic.equals("sztran-key")) {
          producer.send(new ProducerRecord[String, String](topic, fields(2), line))
        } else {
          // 深市三秒行情表
          producer.send(new ProducerRecord[String, String](topic, fields(0), line))
        }
        count += 1
        if (count % 10000 == 0) {
          println("%d records.".format(count))
        }
      }

      println("%d records totally.".format(count))

      source.close()
    }
  }
}
