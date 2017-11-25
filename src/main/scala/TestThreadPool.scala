/**
  * Created by xiaoy on 2017/11/24.
  */

import java.util.concurrent.{Callable, FutureTask, Executors, ExecutorService}

object TestThreadPool {
  def main(args: Array[String]) {
    val threadPool: ExecutorService = Executors.newFixedThreadPool(3)

    val tasks = new Array[FutureTask[String]](4)

    for (i <- 0 to 3) {
      val future = new FutureTask[String](new Callable[String] {
        override def call(): String = {
          Thread.sleep(3000)
          return "im result"
        }
      })
      tasks(i) = future
      threadPool.execute(future)
      println("Task %d launched.".format(i))
    }

    for (i <- 0 to 3){
      println(tasks(i).get())
    }

    threadPool.shutdown()
  }
}