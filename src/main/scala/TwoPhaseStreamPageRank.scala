import java.io.PrintWriter
import java.net.Socket

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.rogach.scallop.ScallopConf

/*
class PageRankListener(val ssc: StreamingContext, val stopIndicator: StopIndicator, val genAddr: String, val port: Int, var ranks: RDD[(Int, List[Double])]) extends StreamingListener {
  var reporter: Option[PrintWriter] = None
  var counter = 0
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
    ranks.saveAsTextFile("save/save" + counter)
    if (stopIndicator.emptyInput >= 2) {
      println("Gracefully stopping Spark Streaming Application")
      println("Summary: ")
      stopIndicator.receives.foreach(x => println("Line received: " + x))
      ssc.stop(true)
      println("Application stopped")
    } else {
      println(s"$counter batch finished")
      counter+=1
      reporter match {
        case None =>
          reporter = Some(new PrintWriter(new Socket(genAddr, port+1).getOutputStream, true))
          reporter.get.print(1)
          reporter.get.flush()
        case Some(r) =>
          r.print(1)
          r.flush()
      }
    }
  }
}


object TwoPhaseStreamPageRank {
  type Scores = List[Double]

  def makeInitial(ssc:StreamingContext, src: String, partition: Int, iters: Int)= {
    val lines = ssc.sparkContext.textFile(src, partition)
    val links = lines.map { s =>
      val parts = s.split("\\s+")
      (parts(0).toInt, parts(1).toInt)
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => List(1.0))

    for (i <- 0 until iters) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank(i) / size))
      }
      ranks = ranks.join(contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)).mapValues {
        case (scores, score) => scores ::: List(score)
      }
    }
    (links, ranks.map {
      case (id, r) => (id, r.drop(1))
    })
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
      val initial = opt[String]("initial", 'i', default = Some("corpus/100kGraph"),
        descr = "The initial states for pages.")
      val duration = opt[Int]("duration", 'd', default = Some(1),
        descr = "The duration of interval of streaming.")
      val partition = opt[Int]("partition", 'r', default = Some(2),
        descr = "THe number of partition of the initial.")
      val iters = opt[Int]("iters", default = Some(5),
        descr = "The number of iterations for computing pagerank.")
    }

    val port = Conf.port()
    val dest = Conf.dest()
    val genAddr = Conf.genAddr()
    val saveOrPrint = Conf.saveOrPrint()
    val initialPath = Conf.initial()
    val partition = Conf.partition()
    val iters = Conf.iters()
    val memLevel = Conf.memLevel() match {
      case "default" => StorageLevel.MEMORY_ONLY_SER
      case x => StorageLevel.fromString(x)
    }

    val duration = Seconds(Conf.duration())
    var isTesting = false
    val stopIndicator = new StopIndicator()


    val sparkConf = new SparkConf()
      .setAppName("StatefulWikiWordCount").setMaster("local[*]")

    // Create the context with given duration
    val ssc = new StreamingContext(sparkConf, duration)
    var stop = false



    var (links, ranks) = makeInitial(ssc, initialPath, partition, iters)
    var delta = ranks.map {
      case (node, ranks) => (node, ranks.map(_ => 0.0))
    }
    //links.cache()
    ssc.addStreamingListener(new PageRankListener(ssc, stopIndicator, genAddr, port, ranks))
    ssc.checkpoint("./CheckPoints/")

    def getNeighbors(id: Int) = links.filter {
        case (node, _) => node == id
      }.map(_._2).first()//.map((id, _))

    def setNeighbors(from: Int, to: Int, op: Int) = {
      op match {
        case 1 => links.map {
          case (node, nei) if (node == nei) => (node, nei ++ Seq(to))
          case x => x
        }
        case -1 => links.map {
          case (node, nei) if (node == nei) => (node, nei.filter(_ != to))
          case x => x
        }
      }
    }

    def duplicate[T](x: T, times: Int): List[T] = times match {
      case 0 => Nil
      case t => x :: duplicate(x, t - 1)
    }

    def getRank(id: Int, iter: Int) = ranks.filter {
      case (node, ranks) => node == id
    }.map(_._2).first()(iter)

    def getDelta(id: Int, iter: Int) = delta.filter {
      case (node, d) => node == id
    }.map(_._2).first()(iter)


    def updateDelta(id: Int, dRank: Double, iter: Int) = {
      delta.map {
        case (node, d) if (node == id) => (node, (d.take(iter) :+ dRank) ::: d.drop(iters - iter + 1))
        case x => x
      }
    }

    def updateDeltaByFrom(from: Int, iter: Int) = {
      val nei = getNeighbors(from)
      val dnei = getDelta(from, iter - 1) / nei.size
      for (node <- nei) {
        updateDelta(node, dnei, iter)
      }
    }

    def update(from: Int, to: Int, op: Int): Unit = {
      links = setNeighbors(from, to, op)
      var nei = getNeighbors(from)
      op match {
        case 1 => {
          val size = nei.size
          val dnei = getRank(from, 0) * (-1 / (size - 1) + 1 / size)
          val dto = getRank(from, 0) / size
          for (node <- nei) {
            delta = updateDelta(node, dnei, 0)
          }
          delta = updateDelta(to, dto, 0)

          for (i <- 1 until iters) {
            for (node <- nei) {
              updateDeltaByFrom(node, i)
            }
            nei = nei.flatMap(getNeighbors(_))
          }
        }
        case -1 => {}
      }
    }

    def updateRecord(record: Array[String]): Unit = {
      val from = record(1).toInt
      val to = record(2).toInt
      val op = record(0).toInt
      update(from, to, op)
    }


 // Create a ReceiverInputDStream on target ip:port and count the
    val lines = ssc.socketTextStream(genAddr, port)

    val updates = lines.map(_.split("\\s+"))

    updates.foreachRDD(rdd => {
      rdd.foreach(updateRecord(_))
    })


    lines.foreachRDD(rdd => {
      rdd.count() match {
        case 0 if isTesting =>
          stopIndicator.receives ++= List(0.toLong)
          stopIndicator.emptyInput += 1
        case x : Long if x != 0 =>
          isTesting = true
          stopIndicator.receives ++= List(x)
          stopIndicator.emptyInput = 0
        case x : Long =>
          stopIndicator.receives ++= List(x)
      }
    })

    saveOrPrint match {
      case 0 => ranks.collect().foreach(println)
      case 1 => ranks.saveAsTextFile(dest)
    }
    ssc.start()
    ssc.awaitTermination()
    println("Terminated.....................")
  }
}
*/