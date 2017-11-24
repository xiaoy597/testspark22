

import java.text.SimpleDateFormat
import java.util.Date

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import org.apache.spark.streaming._

/**
  * Created by xiaoy on 2017/11/22.
  */
object MySparkStreamingApp {
  def main(args: Array[String]): Unit = {

    val context =
      if (args(0).equals("local")) {
        createStreamingContext()
      } else {
        StreamingContext.getOrCreate("/user/root/xiaoy/cp",
          MySparkStreamingApp.functionToCreateContext)
      }

    context.start() // Start the computation
    context.awaitTermination() // Wait for the computation to terminate
  }

  def functionToCreateContext(): StreamingContext = {

    val ssc = createStreamingContext()
    ssc.checkpoint("/user/root/xiaoy/cp") // set checkpoint directory

    ssc
  }

  def createStreamingContext(): StreamingContext = {
    val logger = LoggerFactory.getLogger(MySparkStreamingApp.getClass)

    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("kafka.utils").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local[10]")
      .setAppName("MySparkStreamingApp")
      .set("spark.streaming.kafka.maxRatePerPartition", "100")
    val ssc = new StreamingContext(conf, Seconds(3))
    val topics = "sztran"
    val brokers = "host1:6667,host2:6667,host3:6667,host4:6667,host5:6667"

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "group_of_xiaoy",
      "auto.offset.reset" -> "smallest"
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val recordDStream = messages.map(m => {
      val fields = m._2.split("\\|")
      val now = new Date().getTime
      fields(0) -> (now, fields(7).toFloat, fields(7).toFloat)
    })

    val record3mWindow =
      recordDStream.reduceByKeyAndWindow((x: (Long, Float, Float), y: (Long, Float, Float)) => {
        (Math.min(x._1, y._1), Math.max(x._2, y._2), Math.min(x._3, y._3))
      }, Seconds(30), Seconds(3))
        .filter(x => {
          val v = x._2
          (v._2 - v._3)/v._3 > 0.22
        })

    record3mWindow.foreachRDD(rdd => {
      val now = new Date()
      val format = new SimpleDateFormat("hh:mm:ss.SSS")

      rdd.collect().foreach(x => {
        println("[%s]: %s  %-10.2f %-10.2f %s".format(
          x._1, format.format(now), x._2._2, x._2._3, format.format(new Date(x._2._1))))
      })
    })

    //    record3mWindow.print(2)
    //
    //    recordDStream.foreachRDD((rdd, t) => {
    //      val d = new Date(t.milliseconds)
    //      println("At time %s received %d messages"
    //        .format(new SimpleDateFormat("hh:mm:ss SSS").format(new Date(t.milliseconds)), rdd.count()))
    //    }
    //    )

    ssc
  }

  def setFromOffsets(list: List[(String, Int, Long)]): Map[TopicAndPartition, Long] = {
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (offset <- list) {
      val tp = TopicAndPartition(offset._1, offset._2) //topic和分区数
      fromOffsets += (tp -> offset._3) // offset位置
    }
    fromOffsets
  }
}
