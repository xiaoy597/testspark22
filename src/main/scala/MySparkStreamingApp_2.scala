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

object MySparkStreamingApp_2 {
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

  def updateStateFunc(newValues: Seq[(Long, String, Float)], curState: Option[(List[(Long, Float)], Float, List[(Long, Float)], Float)])
  : Option[(List[(Long, Float)], Float, List[(Long, Float)], Float)] = {

    val newValue:(Long, String, Float) = if (newValues.nonEmpty) newValues.head else (0, null, 0F)

    curState match {
      case None =>
        newValue._2 match {
          case "B" => Some(List((newValue._1, newValue._3)), newValue._3, List(), 0F)
          case "S" => Some(List(), 0F, List((newValue._1, newValue._3)), newValue._3)
        }
      case Some(state) =>
        newValue._2 match {
          case "B" =>
            val newList = ((newValue._1, newValue._3) :: state._1).filter(_._1 >= newValue._1 - 180 * 1000)
            Some(newList, newList.reduce((x1, x2) => (0L, x1._2 + x2._2))._2, state._3, state._4)
          case "S" =>
            val newList = ((newValue._1, newValue._3) :: state._3).filter(_._1 >= newValue._1 - 180 * 1000)
            Some(state._1, state._2, newList, newList.reduce((x1, x2) => (0L, x1._2 + x2._2))._2)
        }
    }
  }

  def createStreamingContext(): StreamingContext = {
    val logger = LoggerFactory.getLogger(MySparkStreamingApp_2.getClass)

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

    val recordDStream = messages.transform(rdd => {
      val now = new Date().getTime
      rdd.map(m => {
        val fields = m._2.split("\\^\\|")
        fields(3) -> (now, fields(4), fields(7).toFloat * fields(8).toFloat)
      })
    })

//    val record3mWindow =
//      recordDStream.reduceByKeyAndWindow((x: (Float, Float), y: (Float, Float)) => {
//        (x._1 + y._1, x._2 + y._2)
//      }, (x: (Float, Float), y: (Float, Float)) => {
//        (x._1 - y._1, x._2 - y._2)
//      }, Seconds(AppConfig.windowWidth.toInt), Seconds(AppConfig.slidingInterval.toInt))
//        .filter(x => x._2._1 >= thresholdBroadcast.value || x._2._2 >= thresholdBroadcast.value)

    val runningState = recordDStream.updateStateByKey(MySparkStreamingApp_2.updateStateFunc)

    runningState.foreachRDD(rdd => {
      val now = new Date()
      val format = new SimpleDateFormat("hh:mm:ss.SSS")
      val monitorListBroadcast = ssc.sparkContext.broadcast(AppConfig.monitorList)

      println("Warnings issued at %s ...".format(format.format(now)))

      rdd.filter(x => monitorListBroadcast.value.contains(x._1)).collect().foreach(x => {

        val row = "%s|%s|%.2f|%.2f|%.2f\n".format(
          format.format(now), x._1, x._2._2, x._2._4,
          if (x._2._2 >= AppConfig.transAmtThreshold3 || x._2._4 >= AppConfig.transAmtThreshold3) {
            AppConfig.transAmtThreshold3
          } else {
            if (x._2._2 >= AppConfig.transAmtThreshold2 || x._2._4 >= AppConfig.transAmtThreshold2) {
              AppConfig.transAmtThreshold2
            } else
              AppConfig.transAmtThreshold1
          }
        )

//        AppConfig.outputStream.write(row.getBytes)
        println(row)

      })

//      AppConfig.outputStream.flush()
    })

    ssc
  }
}
