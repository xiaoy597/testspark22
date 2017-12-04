import scala.io.Source

/**
  * Created by xiaoy on 2017/12/4.
  */
class MonitorObject(val objectSource: String) {
  def objects() : List[String] = {
    val source = Source.fromFile(objectSource)
    val objects = source.getLines().toList
    source.close()
    objects
  }
}
