import scala.collection.mutable

val a = ("abc", "123")
val b = ("abc", "123")

val m = new mutable.HashMap[(String, String), Int]()

m.put(a, 0)

m.contains(a)
m.contains(b)
a == b
a.equals(b)

val s = "B"
val s1 = s match {
  case "B" => "B1"
  case "S" => "S1"
}

val l:List[Int] = List()
l.isEmpty
l.reduce(_ + _)



