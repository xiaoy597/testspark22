/**
  * Created by xiaoy on 2017/11/22.
  */

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object MySparkStreamingApp {
  def main(args: Array[String]): Unit = {

    AppConfig.runMode = args(0)
    AppConfig.brokerList = args(1)
    AppConfig.kafkaTopic = args(2)
    AppConfig.messageRate = args(3)
    AppConfig.windowWidth = args(4)
    AppConfig.slidingInterval = args(5)
    AppConfig.transAmtThreshold1 = args(6).toFloat
    AppConfig.transAmtThreshold2 = args(7).toFloat
    AppConfig.transAmtThreshold3 = args(8).toFloat

    if (args.length > 9)
      AppConfig.monitorList = new MonitorObject(args(9)).objects()

    val context =
      if (AppConfig.runMode.equals("local")) {
        createStreamingContext()
      } else {
        StreamingContext.getOrCreate("/user/root/xiaoy/cp",
          MySparkStreamingApp.functionToCreateContext)
      }

    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val targetPath = new Path("/user/root/xiaoy/test2/test.dat")

    if (hdfs.exists(targetPath))
      hdfs.delete(targetPath, true)

    hdfs.createNewFile(targetPath)
    val outputStream: FSDataOutputStream = hdfs.create(targetPath)

    AppConfig.outputStream = outputStream

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

    val thresholdBroadcast = ssc.sparkContext.broadcast(AppConfig.transAmtThreshold1)


    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val monitorListBroadcast = ssc.sparkContext.broadcast(AppConfig.monitorList)

    val recordDStream = messages.transform(rdd => {
      rdd.map(m => {
        val fields = m._2.split("\\^\\|")
        (fields(3), fields(4)) -> fields(7).toFloat * fields(8).toFloat
      }).filter(x => monitorListBroadcast.value.contains(x._1._1))
    })

    val record3mWindow =
      recordDStream.reduceByKeyAndWindow((x: Float, y: Float) => {
        x + y
      }, (x: Float, y: Float) => {
        x - y
      }, Seconds(AppConfig.windowWidth.toInt), Seconds(AppConfig.slidingInterval.toInt))
        .filter(x => x._2 >= thresholdBroadcast.value)


    record3mWindow.foreachRDD(rdd => {
      val now = new Date()
      val format = new SimpleDateFormat("hh:mm:ss.SSS")

      println("Warnings issued at %s ...".format(format.format(now)))

      rdd.collect().foreach(x => {

        val row = "%s|%s|%.2f|%.2f\n".format(
          format.format(now), x._1, x._2,
          if (x._2 >= AppConfig.transAmtThreshold3) {
            AppConfig.transAmtThreshold3
          } else {
            if (x._2 >= AppConfig.transAmtThreshold2) {
              AppConfig.transAmtThreshold2
            } else
              AppConfig.transAmtThreshold1
          }
        )
        AppConfig.outputStream.write(row.getBytes)
        println(row)
      })
      AppConfig.outputStream.flush()
    })

    ssc
  }
}
