/**
  * Created by xiaoy on 2017/11/27.
  */

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object MyKafkaProducer {
  def main(args: Array[String]) {

    val topic = args(1)
    val dataFile = args(2)
    val serverList = args(0)

    val props = new Properties()
    props.put("bootstrap.servers", serverList)
    props.put("acks", "1")
    props.put("retries", "3")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("compression-codec", "snappy")

    val producer = new KafkaProducer[String, String](props)

    for (batch <- 1 to 5) {
      println("Batch %d begins.".format(batch))

      val source = Source.fromFile(dataFile)
      var count = 0
      for (line <- source.getLines()) {
        val fields = line.split("\\^\\|")
        // 深市过户表
        if (topic.equals("sztran")) {
          producer.send(new ProducerRecord[String, String](topic, fields(3), line))
        } else {
          // 深市三秒行情表
          producer.send(new ProducerRecord[String, String](topic, fields(1), line))
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
