import java.io._
import java.lang._
import java.net.{ServerSocket, Socket, SocketException}
import org.rogach.scallop.ScallopConf

class TestReporter(port: Int) extends Runnable {
  def run(): Unit = {
    try {
      val listener = new ServerSocket(port)
      var counter = 0
      println("TestReporter: Listening on " + port)
      var start = System.nanoTime()
      println("Test begin..")
      val sock = listener.accept()
      val in = sock.getInputStream

      while (in.read() == 49) {
        println(s"TestReporter: $counter batch finished. ")
        counter += 1
        println("Summary: ")
        println("Time took: %f s.".format((System.nanoTime() - start) / 1e9))
        println("----------------------------------")
        start = System.nanoTime()
      }
    }
  }
}


class TwoPhaseCorpusGen(port: Int, source: String, toSent: Int, duration: Float, toTest: Int, emptyRun: Option[Int]) extends Runnable {
  def run() {
    try {
      val listener = new ServerSocket(port)
      var numClients = 1

      println("CorpusGen: Listening on port " + port)

      while (true) {
        new ClitHandler(listener.accept(), numClients, source, toSent, duration, toTest, emptyRun).start()
        println("Starting Test Reporter...")
        new Thread(new TestReporter(port+1)).start()
        numClients += 1
      }

      listener.close()
    }
    catch {
      case e: IOException =>
        System.err.println("Could not listen on port: " + port + ".")
        System.exit(-1)
    }
  }
}

object TwoPhaseDataGen {
  def main(args: Array[String]): Unit = {
    object Conf extends ScallopConf(args) {
      version("Data Gen by Ming")
      val port = opt[Int]("port", 'p', default = Some(9998),
        descr = "The port to listen at.")
      val source = opt[String]("source", 's', default = Some("corpus/100kUpdates"),
        descr = "The source file to read from.")
      val toSent = opt[Int]("toSent", 't', default = Some(5),
        descr = "The number of lines to sent per duration.")
      val duration = opt[Int]("duration", 'd', default = Some(1000),
        descr = "The time duration to send data. (ms).")
      val toTest = opt[Int]("toTest", 'r', default = Some(3),
        descr = "The number of durations to run the test.")
      val emptyRun = opt[Int]("emtpyRun", 'e', default = Some(1),
        descr = "The number of runs of empty data before sending the first batch. Empty batch allows Spark Stream to do initialization.")
    }

    val port = Conf.port()
    val source = Conf.source()
    val toSent = Conf.toSent()
    val duration = Conf.duration().toFloat
    val toTest = Conf.toTest()
    val emptyRun = Conf.emptyRun() match {
      case 0 => None
      case x => Some(x)
    }


    new Thread(new TwoPhaseCorpusGen(port, source, toSent, duration, toTest, emptyRun)).start()
    println("Two Phase Corpus Generator started")
  }
}