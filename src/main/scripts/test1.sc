import scala.collection.mutable

val a = ("abc", "123")
val b = ("abc", "123")

val m = new mutable.HashMap[(String, String), Int]()

m.put(a, 0)

m.contains(a)
m.contains(b)
a == b
a.equals(b)


