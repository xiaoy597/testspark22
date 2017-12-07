import org.apache.hadoop.fs.FileSystem
import org.apache.spark.broadcast.Broadcast

/**
  * Created by xiaoy on 2017/11/25.
  */
object AppConfig {
  var brokerList:String = ""
  var runMode = "local"
  var messageRate = "100"
  var windowWidth = "180"
  var slidingInterval = "2"
  var kafkaTopic = "sztran-key"
  var consumerGroup = ""
  var vibrateThreshold1:Float = 0
  var vibrateThreshold2:Float = 0
  var vibrateThreshold3:Float = 0
  var transAmtPctThreshold1:Float = 0
  var transAmtPctThreshold2:Float = 0
  var transAmtPctThreshold3:Float = 0
  var transAmtThreshold1:Float = 0
  var transAmtThreshold2:Float = 0
  var transAmtThreshold3:Float = 0
  var monitorList:List[String] = List.empty[String]
  var messageStore:String = ""
}
