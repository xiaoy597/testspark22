

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Callable, ExecutorService, Executors, FutureTask}

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by xiaoy on 2017/11/22.
  */
object MySparkStreamingApp {
  def main(args: Array[String]): Unit = {

    AppConfig.runMode = args(0)
    AppConfig.brokerList = args(1)
    AppConfig.kafkaTopic = args(2)
    AppConfig.messageRate = args(3)
    AppConfig.windowWidth = args(4)
    AppConfig.slidingInterval = args(5)
    AppConfig.threashold1 = args(6).toFloat
    AppConfig.threashold2 = args(7).toFloat
    AppConfig.threashold3 = args(8).toFloat

    val context =
      if (AppConfig.runMode.equals("local")) {
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
      .setAppName("MySparkStreamingApp")
      .set("spark.streaming.kafka.maxRatePerPartition", AppConfig.messageRate)

    if (AppConfig.runMode.equals("local")) {
      conf.setMaster("local[20]")
    }

    val ssc = new StreamingContext(conf, Seconds(1))
    val topics = AppConfig.kafkaTopic

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> AppConfig.brokerList,
      "group.id" -> "group_of_xiaoy",
      "auto.offset.reset" -> "smallest"
    )

    val thresholdBroadcast = ssc.sparkContext.broadcast(AppConfig.threashold1)


    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val recordDStream = messages.transform(rdd => {
      val now = new Date().getTime
      rdd.map(m => {
        val fields = m._2.split("\\|")
        (fields(2), fields(3)) -> (now, fields(6).toFloat * fields(7).toFloat)
      })
    })

    val record3mWindow =
      recordDStream.reduceByKeyAndWindow((x: (Long, Float), y: (Long, Float)) => {
        (Math.max(x._1, y._1), x._2 + y._2)
      }, Seconds(AppConfig.windowWidth.toInt), Seconds(AppConfig.slidingInterval.toInt))
        .filter(x => x._2._2 >= thresholdBroadcast.value)

//    val oldWarnings = new mutable.HashMap[(String, String), (Long, Float)]()

    record3mWindow.foreachRDD(rdd => {
      val now = new Date()
      val format = new SimpleDateFormat("hh:mm:ss.SSS")

      println("Warnings issued at %s ...".format(format.format(now)))

      rdd.collect().foreach(x => {
//        if (oldWarnings.contains(x._1)) {
//          if (x._2._1 - oldWarnings(x._1)._1 > 60 * 1000) {
//            oldWarnings(x._1) = x._2
//            println("[%s]: %20.2f %20s".format(
//              x._1, x._2._2, format.format(new Date(x._2._1))))
//          }
//        } else {
//          oldWarnings(x._1) = x._2
//          println("[%s]: %20.2f %20s".format(
//            x._1, x._2._2, format.format(new Date(x._2._1))))
//        }

        println("[%s]: %20.2f, Warning Level %.2f, Last transaction at %s".format(
          x._1, x._2._2,
          if (x._2._2 >= AppConfig.threashold3) {
            AppConfig.threashold3
          } else {
            if (x._2._2 >= AppConfig.threashold2) {
              AppConfig.threashold2
            } else
              AppConfig.threashold1
          },
          format.format(new Date(x._2._1))
        ))
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
