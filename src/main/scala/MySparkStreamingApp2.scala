/**
  * Created by xiaoy on 2017/11/28.
  */


import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object MySparkStreamingApp2 {
  def main(args: Array[String]): Unit = {

    AppConfig.runMode = args(0)
    AppConfig.brokerList = args(1)
    AppConfig.kafkaTopic = args(2)
    AppConfig.messageRate = args(3)
    AppConfig.windowWidth = args(4)
    AppConfig.slidingInterval = args(5)
    AppConfig.vibrateThreshold1 = args(6).toFloat
    AppConfig.vibrateThreshold2 = args(7).toFloat
    AppConfig.vibrateThreshold3 = args(8).toFloat
    AppConfig.transAmtPctThreshold1 = args(9).toFloat
    AppConfig.transAmtPctThreshold2 = args(10).toFloat
    AppConfig.transAmtPctThreshold3 = args(11).toFloat

    val context =
      if (AppConfig.runMode.equals("local")) {
        createStreamingContext()
      } else {
        StreamingContext.getOrCreate("/user/root/xiaoy/cp",
          MySparkStreamingApp2.functionToCreateContext)
      }

    context.start() // Start the computation
    context.awaitTermination() // Wait for the computation to terminate
  }

  def functionToCreateContext(): StreamingContext = {

    val ssc = createStreamingContext()
    ssc.checkpoint("/user/root/xiaoy/cp") // set checkpoint directory

    ssc
  }


  def updateStateFunc(newValues: Seq[(Long, Float, Float, Float)], curState: Option[(Float, Float, Float)])
  : Option[(Float, Float, Float)] = {

    val newValue:(Long, Float, Float, Float) = if (newValues.nonEmpty) newValues.head else (0, 0, 0, 0)
    val vibrateRate:Float = (newValue._4 - newValue._3) / (if (newValue._3 != 0) newValue._3 else 1)

    curState match {
      case None =>
        Some(newValue._1, newValue._1, vibrateRate)
      case Some(state) =>
        Some(newValue._1, newValue._1 + state._2, vibrateRate)
    }
  }

  def createStreamingContext(): StreamingContext = {
    val logger = LoggerFactory.getLogger(MySparkStreamingApp2.getClass)

    Logger.getLogger("org.apache.kafka").setLevel(Level.WARN)
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("kafka.utils").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("MySparkStreamingApp2")
      .set("spark.streaming.kafka.maxRatePerPartition", AppConfig.messageRate)

    if (AppConfig.runMode.equals("local")) {
      conf.setMaster("local[20]")
    }

    val ssc = new StreamingContext(conf, Seconds(1))
    val topics = AppConfig.kafkaTopic

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> AppConfig.brokerList,
      "group.id" -> "group2_of_xiaoy",
      "auto.offset.reset" -> "smallest"
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val recordDStream = messages.transform(rdd => {
      val now = new Date().getTime
      rdd.map(m => {
        val fields = m._2.split("\\|")
        fields(0) -> (now, fields(5).toFloat, fields(13).toFloat, fields(13).toFloat)
      })
    })

    val vibrateThresholdBroadcast = ssc.sparkContext.broadcast(AppConfig.vibrateThreshold1)
    val transAmtPctThresholdBroadcast = ssc.sparkContext.broadcast(AppConfig.transAmtPctThreshold1)

    val record3mWindow =
      recordDStream.reduceByKeyAndWindow((x: (Long, Float, Float, Float), y: (Long, Float, Float, Float)) => {
        (Math.max(x._1, y._1), x._2 + y._2, Math.min(x._3, y._3), Math.max(x._4, y._4))
      }, Seconds(AppConfig.windowWidth.toInt), Seconds(AppConfig.slidingInterval.toInt))

    val runningState =
      record3mWindow.updateStateByKey(MySparkStreamingApp2.updateStateFunc)

    runningState.foreachRDD(rdd => {
      val now = new Date()
      val format = new SimpleDateFormat("hh:mm:ss.SSS")

      println("Warnings issued at %s ...".format(format.format(now)))

      rdd.filter(x => {
        x._2._1 / x._2._2 >= transAmtPctThresholdBroadcast.value &&
          x._2._3 >= vibrateThresholdBroadcast.value
      }).collect().foreach(x => {

        val transAmtPct = x._2._1 / x._2._2
        val vibrateRate = x._2._3

        println("[%s]: TransAmtPct: %6.2f(%.2f), VibrateRate: %6.2f(%.2f)".format(
          x._1, transAmtPct,
          if (transAmtPct >= AppConfig.transAmtPctThreshold3) {
            AppConfig.transAmtPctThreshold3
          } else {
            if (transAmtPct >= AppConfig.transAmtPctThreshold2) {
              AppConfig.transAmtPctThreshold2
            } else
              AppConfig.transAmtPctThreshold1
          },
          vibrateRate,
          if (vibrateRate >= AppConfig.vibrateThreshold3) {
            AppConfig.vibrateThreshold3
          } else {
            if (vibrateRate >= AppConfig.vibrateThreshold2) {
              AppConfig.vibrateThreshold2
            } else
              AppConfig.vibrateThreshold1
          }
        ))
        println("[%s]: TransAmtLast3m=%.2f, TransAmtTotal=%.2f".format(
          x._1, x._2._1, x._2._2
        ))

      })
    })

    ssc
  }
}
