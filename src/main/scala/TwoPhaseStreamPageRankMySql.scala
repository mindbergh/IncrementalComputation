import java.io.PrintWriter
import java.net.Socket
import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{HashPartitioner, SparkConf}
import org.rogach.scallop.ScallopConf

class PageRankListener(val ssc: StreamingContext, val stopIndicator: StopIndicator, val genAddr: String, val port: Int, var ranks: RDD[(Int, List[Double])]) extends StreamingListener {
  var reporter: Option[PrintWriter] = None
  var counter = 0
  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
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


object TwoPhaseStreamPageRankMySql {
  type Scores = List[Double]

  def makeInitial(ssc: StreamingContext, src: String, partition: Int, iters: Int) = {
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

  class MySqlHanler(url: String, username: String, password: String) extends Serializable {
    val conn = DriverManager.getConnection(url, username, password)
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
    val start = System.nanoTime()

    val sparkConf = new SparkConf()
      .setAppName("StatefulWikiWordCount").setMaster("local[*]")

    // Create the context with given duration
    val ssc = new StreamingContext(sparkConf, duration)
    var stop = false


    val url = "jdbc:mysql://10.211.58.101:3306/pr"
    val username = "root"
    val password = "PASSWORD"
    Class.forName("com.mysql.jdbc.Driver").newInstance

    /*
    val myRDD = new JdbcRDD(ssc.sparkContext, () =>
      DriverManager.getConnection(url, username, password),
      "select first_name,last_name,gender from person limit ?, ?",
      1, 5, 2, r => r.getString("last_name") + ", " + r.getString("first_name"))
    */

    val (links, ranks) = makeInitial(ssc, initialPath, partition, iters)
    val delta = ranks.map {
      case (node, ranks) => (node, ranks.map(_ => 0.0))
    }

    val conn = DriverManager.getConnection(url, username, password)
    val clearLinks = conn.prepareStatement("DELETE FROM links;")
    val clearRanks = conn.prepareStatement("DELETE FROM ranks;")
    val clearDeltas = conn.prepareStatement("DELETE FROM deltas;")
    clearLinks.executeUpdate()
    clearRanks.executeUpdate()
    clearDeltas.executeUpdate()
    conn.close()

    links.foreachPartition { it =>
      val conn = DriverManager.getConnection(url, username, password)
      val del = conn.prepareStatement("INSERT INTO links VALUES(?, ?);")
      for ((node, nei) <- it) {
        del.setInt(1, node)
        del.setString(2, nei.mkString(" "))
        del.executeUpdate()
      }
      conn.close()
    }

    ranks.foreachPartition { it =>
      val conn = DriverManager.getConnection(url, username, password)
      val del1 = conn.prepareStatement("INSERT INTO ranks VALUES(?, ?, ?, ?, ?, ?);")
      val del2 = conn.prepareStatement("INSERT INTO deltas VALUES(?, 0.0, 0.0, 0.0, 0.0, 0.0);")
      for ((node, r) <- it) {
        del1.setInt(1, node)
        del1.setDouble(2, r(0))
        del1.setDouble(3, r(1))
        del1.setDouble(4, r(2))
        del1.setDouble(5, r(3))
        del1.setDouble(6, r(4))
        del2.setInt(1, node)
        del1.executeUpdate()
        del2.executeUpdate()
      }
      conn.close()
    }

    println("Initial time took %f".format((System.nanoTime() - start) / 1e9))
    //links.cache()
    ssc.addStreamingListener(new PageRankListener(ssc, stopIndicator, genAddr, port, ranks))
    ssc.checkpoint("./CheckPoints/")

    def getNeighbors(id: Int, dbh: MySqlHanler): Array[Int] = {
      //println("getNeighbor")
      val del = dbh.conn.prepareStatement(s"SELECT neighbors FROM links WHERE node = $id;")
      val res = del.executeQuery()
      if (res.next() == false) Array()
      else res.getString("neighbors").split("\\s+").map(_.toInt)
    }

    def setNeighbors(from: Int, to: Int, op: Int, dbh: MySqlHanler) = {
      //println("setNeighbors")
      val conn = dbh.conn
      op match {
        case 1 => {
          val del = conn.prepareStatement(s"SELECT neighbors FROM links WHERE node = $from;")
          val res = del.executeQuery()
          if (res.next()) {
            val nei = res.getString("neighbors").split("\\s+").map(_.toInt)
            val newNei = (nei :+ to).mkString(" ")
            val upd = conn.prepareStatement(s"UPDATE links SET neighbors = '$newNei' WHERE node = $from")
            upd.executeUpdate()
          } else {
            val upd = conn.prepareStatement(s"INSERT INTO links VALUES($from, '$to');")
            upd.executeUpdate()
          }
        }
        case -1 => {
          val del = conn.prepareStatement(s"SELECT neighbors FROM links WHERE node = $from;")
          val res = del.executeQuery()
          if (res.next()) {
            val nei = res.getString("neighbors").split("\\s+").map(_.toInt)
            val newNei = nei.filter(_ != to).mkString(" ")
            val upd = conn.prepareStatement(s"UPDATE links SET neighbors = '$newNei' WHERE node = $from")
            upd.executeUpdate()
          }
        }
      }
    }

    def getRank(id: Int, iter: Int, dbh: MySqlHanler): Double = {
      //println("getRank")
      val stmt = dbh.conn.prepareStatement(s"SELECT i$iter FROM ranks WHERE node = $id")
      val res = stmt.executeQuery()
      res.next()
      val r = res.getDouble(s"i$iter")
      r
    }

    def getDelta(id: Int, iter: Int, dbh: MySqlHanler) = {
      //println("getDelta")
      val stmt = dbh.conn.prepareStatement(s"SELECT i$iter FROM deltas WHERE node = $id")
      val res = stmt.executeQuery()
      res.next()
      val r = res.getDouble(s"i$iter")
      r
    }
    def updateDelta(id: Int, dRank: Double, iter: Int, dbh: MySqlHanler) = {
      //println("updateDelta")
      val stmt = dbh.conn.prepareStatement(s"UPDATE deltas SET i$iter = $dRank WHERE node = $id")
      stmt.executeUpdate()
    }

    def updateDeltaByFrom(from: Int, iter: Int, dbh: MySqlHanler) = {
      //println("updateDeltaByFrom")
      val nei = getNeighbors(from, dbh)
      val dnei = getDelta(from, iter - 1, dbh) / nei.size
      val snei = nei.mkString(",")
      val stmt = dbh.conn.prepareStatement(s"UPDATE deltas SET i$iter = $dnei WHERE node IN ($snei)")
      stmt.executeUpdate()
    }

    def update(from: Int, to: Int, op: Int, dbh: MySqlHanler): Unit = {
      setNeighbors(from, to, op, dbh)
      var nei = getNeighbors(from, dbh)
      op match {
        case 1 => {
          val size = nei.size
          val dnei = getRank(from, 0, dbh) * (-1 / (size - 1) + 1 / size)
          val dto = getRank(from, 0, dbh) / size
          for (node <- nei) {
            updateDelta(node, dnei, 0, dbh)
          }
          updateDelta(to, dto, 0, dbh)

          for (i <- 1 until iters) {
            println("Nei size = " + nei.size)
            for (node <- nei) {
              updateDeltaByFrom(node, i, dbh)
            }
            nei = nei.flatMap(getNeighbors(_, dbh)).distinct
          }
        }
        case -1 => {
          val size = nei.size
          val dnei = getRank(from, 0, dbh) * (-1 / (size + 1) + 1 / size)
          for (node <- nei) {
            updateDelta(node, dnei, 0, dbh)
          }

          for (i <- 1 until iters) {
            println("Nei size = " + nei.size)
            for (node <- nei) {
              updateDeltaByFrom(node, i, dbh)
            }
            nei = nei.flatMap(getNeighbors(_, dbh)).distinct
          }
        }
      }
    }

    def updateRecord(record: Array[String]): Unit = {
      val dbh = new MySqlHanler(url, username, password)
      val from = record(1).toInt
      val to = record(2).toInt
      val op = record(0).toInt
      update(from, to, op, dbh)
    }


    // Create a ReceiverInputDStream on target ip:port and count the
    val lines = ssc.socketTextStream(genAddr, port)

    val updates = lines.map(_.split("\\s+"))
    var i = 0
    updates.foreachRDD(
      rdd => {
      println("New RDD")
      rdd.foreach(updateRecord(_))
      //conn.close()
    })


    lines.foreachRDD(rdd => {
      rdd.count() match {
        case 0 if isTesting =>
          stopIndicator.receives ++= List(0.toLong)
          stopIndicator.emptyInput += 1
        case x: Long if x != 0 =>
          isTesting = true
          stopIndicator.receives ++= List(x)
          stopIndicator.emptyInput = 0
        case x: Long =>
          stopIndicator.receives ++= List(x)
      }
    })

    /*
    saveOrPrint match {
      case 0 => ranks.collect().foreach(println)
      case 1 => ranks.saveAsTextFile(dest)
    }
    */
    ssc.start()
    ssc.awaitTermination()
    println("Terminated.....................")
  }
}
