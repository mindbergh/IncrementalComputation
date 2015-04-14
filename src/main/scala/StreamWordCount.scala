import java.io.PrintWriter
import java.net.Socket

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming._
import org.rogach.scallop.ScallopConf

class MyListener(val ssc: StreamingContext, val stopIndicator: StopIndicator) extends StreamingListener {
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
    if (stopIndicator.isStop) {
      println("Gracefully stopping Spark Streaming Application")
      ssc.stop()
      println("Application stopped")
    }
  }
}
class StopIndicator extends Serializable {
  var isStop: Boolean = false
  var emptyInput: Int = 0
  var receives: List[Long] = Nil
}

object StreamWordCount {
  type Count = Map[String, Int]

  def makeInitial(ssc:StreamingContext, src: String) : RDD[(String, Count)] = {
    val Separator = 31.toChar.toString
    val textfile = ssc.sparkContext.textFile(src)
    textfile
      .map(x => x.split(Separator))
      .map(x => (x(0), x(1)
        .split(" ")
        .map(x => (x, 1))
      .groupBy(_._1)
      .map { case (word: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }))
  }


  def main(args: Array[String]) = {
    object Conf extends ScallopConf(args) {
      version("Stream word count")
      val port = opt[Int]("port", 'p', default = Some(9998),
        descr = "The port to listen at.")
      val dest = opt[String]("dest", 'o', default = Some("./save/save"),
        descr = "The path to save outputs.")
      val genAddr = opt[String]("genAddr", 'h', default = Some("localhost"),
        descr = "The address of the load generator.")
      val memLevel = opt[String]("memLevel", 'm', default = Some("default"),
        descr = "The persistence level of DStream.")
      val saveOrPrint = opt[Int]("saveOrPrint", 's', default = Some(1),
        descr = "Save the result or print it. 0 = print; 1 = save.")
      val initial = opt[String]("initial", 'i', default = Some("corpus/initial"),
        descr = "The initial states for pages.")
    }

    val port = Conf.port()
    val dest = Conf.dest()
    val genAddr = Conf.genAddr()
    val saveOrPrint = Conf.saveOrPrint()
    val initialPath = Conf.initial()
    //val initialPath = ""
    val memLevel = Conf.memLevel() match {
      case "default" => StorageLevel.MEMORY_ONLY_SER
      case x => StorageLevel.fromString(x)
    }

    val Separator = 31.toChar.toString
    val duration = Seconds(1)
    var isTesting = false
    val stopIndicator = new StopIndicator()

    val updateFunc = (values: Seq[Count], state: Option[Count]) =>
      values.length match {
        case 0 => state
        case _ => Some(values.last)
      }

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Count], Option[Count])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    val sparkConf = new SparkConf()
      .setAppName("StatefulWikiWordCount").setMaster("local[*]")

    // Create the context with given duration
    val ssc = new StreamingContext(sparkConf, duration)
    var stop = false
    ssc.addStreamingListener(new MyListener(ssc, stopIndicator))
    ssc.checkpoint("./CheckPoints/")


    // Initial RDD input to updateStateByKey
    val start = System.nanoTime()
    val initialRDD = initialPath match {
      case "" => ssc.sparkContext.parallelize(List(): List[(String, Count)])
      case path => makeInitial(ssc, path)
    }
    //initialRDD.persist(StorageLevel.MEMORY_ONLY)
    //initialRDD.foreach(x => {})
    println("Initial time took %f".format((System.nanoTime() - start) / 1e9))
    // Create a ReceiverInputDStream on target ip:port and count the
    val lines = ssc.socketTextStream(genAddr, port)

    val pageDstream = lines
      .map(x => x.split(Separator))
      .map(x => (x(0), x(1)
        .split(" ")
        .map(x => (x, 1))
        .groupBy(_._1)
        .map { case (word: String, traversable) => traversable.reduce { (a, b) => (a._1, a._2 + b._2) } }))

    val stateDstream = pageDstream.updateStateByKey[Count](newUpdateFunc,
                                                           new HashPartitioner (ssc.sparkContext.defaultParallelism),
                                                           true, initialRDD)
    lines.foreachRDD(rdd => {
      rdd.count() match {
        case 0 if isTesting => {
          println("Receive: 0.")
          val stopAsker = new PrintWriter(new Socket(genAddr, port+1).getOutputStream, true)
          stopAsker.print(1)
          stopIndicator.isStop = true
        }
        case x if x != 0 => {
          println("Receive: " + x)
          isTesting = true
        } //stateDstream.print()
        case x => println("Receive: " + x)
      }
    })

    saveOrPrint match {
      case 0 => stateDstream.print
      case 1 => stateDstream.saveAsTextFiles(dest)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}