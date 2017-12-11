import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, MyKafkaManager, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import org.slf4j.LoggerFactory

/**
  * Created by xiaoy on 2017/12/7.
  */
object MyKafkaMessageDumper {
  def main(args: Array[String]): Unit = {

    AppConfig.runMode = args(0)
    AppConfig.brokerList = args(1)
    AppConfig.kafkaTopic = args(2)
    AppConfig.consumerGroup = args(3)
    AppConfig.messageRate = args(4)
    AppConfig.messageStore = args(5)

    val context = createStreamingContext()

    context.start() // Start the computation
    context.awaitTermination() // Wait for the computation to terminate
  }

  def createStreamingContext(): StreamingContext = {
    val logger = LoggerFactory.getLogger(MyKafkaMessageDumper.getClass)

    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("kafka.utils").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("MyKafkaMessageDumper")
      .set("spark.streaming.kafka.maxRatePerPartition", AppConfig.messageRate)

    if (AppConfig.runMode.equals("local")) {
      conf.setMaster("local[20]")
    }

    val ssc = new StreamingContext(conf, Seconds(10))
    val topics = AppConfig.kafkaTopic

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> AppConfig.brokerList,
      "group.id" -> AppConfig.consumerGroup,
      "auto.offset.reset" -> "smallest"
    )
    val km = new MyKafkaManager(kafkaParams)

    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val rowCount = ssc.sparkContext.longAccumulator("rowCount")
    val msgStoreBroadcast = ssc.sparkContext.broadcast(AppConfig.messageStore)

    messages.foreachRDD(rdd => {
      val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val now = new Date().getTime

      println("Writing to database ...")
      for (o <- offsetsList) {
        if (o.fromOffset != o.untilOffset)
          println(msgStoreBroadcast.value + Path.SEPARATOR
            + s"${o.partition}-${o.fromOffset}-${o.untilOffset}")
      }

      rdd.foreachPartition(part => {
        if (part.nonEmpty) {
          val hdfs: FileSystem = FileSystem.get(new Configuration)
          val o: OffsetRange = offsetsList(TaskContext.get.partitionId)
          val targetPath = new Path(msgStoreBroadcast.value + Path.SEPARATOR
            + s"${o.partition}-${o.fromOffset}-${o.untilOffset}")

          if (hdfs.exists(targetPath))
            hdfs.delete(targetPath, true)

          hdfs.createNewFile(targetPath)

          val outputStream: FSDataOutputStream = hdfs.create(targetPath)
          part.foreach(row => {
            outputStream.write((row._2 + "\n").getBytes)
            rowCount.add(1L)
          })
          outputStream.close()
        }
      })
      println(s"${rowCount.value} rows written.")
      km.updateZKOffsets(rdd)

    })

    ssc
  }
}
