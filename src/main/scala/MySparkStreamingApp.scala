

import java.text.SimpleDateFormat
import java.util.Date

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by xiaoy on 2017/11/22.
  */
object MySparkStreamingApp {
  def main(args: Array[String]): Unit = {
    //    val logger = LoggerFactory.getLogger(MySparkStreamingApp.getClass)
    //
    //    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //    Logger.getLogger("kafka.utils").setLevel(Level.WARN)
    //
    //    val conf = new SparkConf()
    //      .setMaster("local[10]")
    //      .setAppName("MySparkStreamingApp")
    //      .set("spark.streaming.kafka.maxRatePerPartition", "250000")
    //    val ssc = new StreamingContext(conf, Seconds(5))

    //    val lines = ssc.socketTextStream("localhost", 5678)
    //
    //    val words = lines.flatMap(_.split(" "))
    //
    //    val pairs = words.map(word => (word, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)
    //
    //    wordCounts.print()

    //    val topics = "test7"
    //    val brokers = "host1:6667,host2:6667,host3:6667,host4:6667,host5:6667"
    //
    //    val topicsSet = topics.split(",").toSet
    //    val kafkaParams = Map[String, String](
    //      "metadata.broker.list" -> brokers,
    //      "group.id" -> "group_of_xiaoy",
    //      "auto.offset.reset" -> "smallest"
    //    )
    //
    //    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
    //      ssc, kafkaParams, topicsSet)

    //    val offsetList = List((topics, 0, 22753623L),(topics, 1, 327041L))                          //指定topic，partition_no，offset
    //    val fromOffsets = setFromOffsets(offsetList)     //构建参数
    //
    //    val messageHandler = (mam: MessageAndMetadata[String, String]) => (mam.topic, mam.message()) //构建MessageAndMetadata
    //
    //    val messages: InputDStream[(String, String)]  =
    //      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)]
    //    (ssc, kafkaParams, fromOffsets, messageHandler)


    //    val lines = messages.map(_._2)


    val context = StreamingContext.getOrCreate("/user/root/xiaoy/cp",
      MySparkStreamingApp.functionToCreateContext)

    context.start() // Start the computation
    context.awaitTermination() // Wait for the computation to terminate
  }

  def functionToCreateContext(): StreamingContext = {
    val logger = LoggerFactory.getLogger(MySparkStreamingApp.getClass)

    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("kafka.utils").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local[10]")
      .setAppName("MySparkStreamingApp")
      .set("spark.streaming.kafka.maxRatePerPartition", "1")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = "test7"
    val brokers = "host1:6667,host2:6667,host3:6667,host4:6667,host5:6667"

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "group_of_xiaoy",
      "auto.offset.reset" -> "smallest"
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(m => m._2.split("\\^\\|")(0) -> m._2)
//    val lines = messages.map(m => m._2)

    lines.print(2)

    lines.foreachRDD((rdd, t) => {
      val d = new Date(t.milliseconds)
      println("At time %s received %d messages"
        .format(new SimpleDateFormat("hh:mm:ss SSS").format(new Date(t.milliseconds)), rdd.count()))
    }
    )

    ssc.checkpoint("/user/root/xiaoy/cp") // set checkpoint directory

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
