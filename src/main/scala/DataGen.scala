import java.io._
import java.lang._
import java.net.{ServerSocket, Socket, SocketException}
import org.rogach.scallop.ScallopConf
import scala.actors.Actor
import scala.io.Source

class TestStoper(port: Int) extends Runnable {
  def run(): Unit = {
    try {
      val listener = new ServerSocket(port)
      println("TestStoper: Listening on " + port)
      val start = System.nanoTime()
      println("Test begin..")
      val sock = listener.accept()
      val in = sock.getInputStream
      in.read()
      println("TestStoper: test finished. ")
      println("Summary: ")
      println("Time took: %f s.".format((System.nanoTime() - start) / 1e9))
    }
  }
}


class CorpusGen(port: Int, source: String, toSent: Int, duration: Float, toTest: Int) extends Runnable {
  def run() {
    try {
      val listener = new ServerSocket(port)
      var numClients = 1

      println("CorpusGen: Listening on port " + port)

      while (true) {
        new ClitHandler(listener.accept(), numClients, source, toSent, duration, toTest).start()
        println("Starting Test Stopper...")
        new Thread(new TestStoper(port+1)).start()
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

class ClitHandler(socket: Socket, clientId: Int, source: String, toSent: Int, duration: Float, toTest: Int) extends Actor {
  private def sendData(iter: Iterator[String], out: PrintWriter, toSent: Int): Unit = {
    toSent match {
      case 0 => return
      case _ if iter.hasNext => {
        out.println(iter.next)
        sendData(iter, out, toSent - 1)
      }
      case _ => return
    }
  }
  private def runTest(iter: Iterator[String], out: PrintWriter, toTest: Int): Unit = {
    toTest match {
      case 0 => return
      case _ => {
        val start = System.nanoTime()
        val startEpoch = System.currentTimeMillis
        sendData(iter, out, toSent)
        val elapsed = (System.nanoTime() - start) / 1e6
        (duration - elapsed) match {
          case x if x > 0 => {
            println("Start time: %d, Time took: %f ms, About to sleep %f ms. toTest: %d".format(startEpoch, elapsed, x, toTest))
            Thread.sleep(x.toLong)
          }
          case _ => {
            println("Start time: %d, Time took: %f ms, About to sleep 0 ms, toTest: %d".format(startEpoch, elapsed, toTest))
          }
        }
        runTest(iter, out, toTest - 1)
      }
    }
  }

  def act {
    try {
      val out = new PrintWriter(socket.getOutputStream(), true)
      val in = new BufferedReader(new InputStreamReader(System.in))
      val srcPath = source

      print("Spark stream connected from " + socket.getInetAddress() + ":" + socket.getPort)
      println(" assigning id " + clientId)
      println("Data generating rate: %d lines per %f ms.".format(toSent, duration))
      println("Number of iterations: %d.".format(toTest))

      val iter = Source.fromFile(srcPath).getLines()

      runTest(iter, out, toTest)

      println("DataGen: TestDate all sent.")
      in.read()
    }
    catch {
      case e: SocketException =>
        System.err.println(e)

      case e: IOException =>
        System.err.println(e.printStackTrace())

      case e: Throwable =>
        System.err.println("Unknown error " + e)
    }
  }
}

object DataGenMain {
  def main(args: Array[String]): Unit = {
    object Conf extends ScallopConf(args) {
      version("Data Gen by Ming")
      val port = opt[Int]("port", 'p', default = Some(9998),
        descr = "The port to listen at.")
      val source = opt[String]("source", 's', default = Some("corpus/increWithHis"),
        descr = "The source file to read from.")
      val toSent = opt[Int]("toSent", 't', default = Some(100),
        descr = "The number of lines to sent per duration.")
      val duration = opt[Int]("duration", 'd', default = Some(1000),
        descr = "The time duration to send data. (ms).")
      val toTest = opt[Int]("toTest", 'r', default = Some(5),
        descr = "The number of durations to run the test.")
    }

    val port = Conf.port()
    val source = Conf.source()
    val toSent = Conf.toSent()
    val duration = Conf.duration().toFloat
    val toTest = Conf.toTest()


    new Thread(new CorpusGen(port, source, toSent, duration, toTest)).start()
    println("Server started")
  }
}