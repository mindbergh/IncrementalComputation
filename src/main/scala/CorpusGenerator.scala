import java.io._
import java.net.{ServerSocket, Socket, SocketException}
import scala.actors.Actor
import scala.io.Source


class Generator(val port: Int) extends Runnable {

  def run() {
    try {
      val listener = new ServerSocket(port)
      var numClients = 1

      println("Listening on port " + port)

      while (true) {
        new ClientHandler(listener.accept(), numClients).start()
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

class ClientHandler(socket: Socket, clientId: Int) extends Actor {
  def act {
    try {

      val out = new PrintWriter(socket.getOutputStream(), true)
      val in = new BufferedReader( new InputStreamReader(System.in))
      val srcPath = "corpus/whole"

      print("Client connected from " + socket.getInetAddress() + ":" + socket.getPort)
      println(" assigning id " + clientId)

      //val inputLine = Array("Bridget" + 30.toChar.toString + "Hello World Hello World Hello World Hello World Hello World Hello",
      //                      "Bridget" + 30.toChar.toString + "Hello World Hello Hello World Hello World Hello")
      val source = Source.fromFile(srcPath)
      for (line <- source.getLines()) {
        line.length
        out.println(line)
        Thread.sleep(200)
      }

      /*
      var count = 0
      var inputLine = in.readLine()
      while (inputLine != null) {
        println(clientId + ") " + count)
        out.println(inputLine(count % 2))
        //inputLine = in.readLine()
        Thread.sleep(3000)
        count = count + 1
      }
      */
      socket.close()

      println("Client " + clientId + " quit")
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
 
object CorpusGeneratorMain {
  def main(args: Array[String]): Unit = {
    val serv = new Thread(new Generator(9998))
    serv.start()
    println("Server started")
    /*
    val source = Source.fromURL(getClass.getResource("incre"))
    val delim = 31.toChar.toString

    for (line <- source.getLines()) {
      println(line.split(delim)(1))

      System.exit(0)
    }
    */
  }
}