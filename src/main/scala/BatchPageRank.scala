import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.ScallopConf

/**
 * Created by fangming on 3/20/15.
 */
object BatchPageRank {

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("Batch Page Rank by Ming")
      val source = opt[String]("source", 'f', default = Some("corpus/100Kgraph"),
        descr = "The source file to read from.")
      val iters = opt[Int]("iters", 't', default = Some(5),
        descr = "The number of iterations")
      val damping = opt[String]("dumping", 'd', default = Some("0.85"),
        descr = "The damping factor.")
      val partition = opt[Int]("partition", 'p', default = Some(1),
        descr = "The number of partition to store input.")
      val saveOrPrint = opt[Int]("saveOrPrint", 's', default = Some(2),
        descr = "Save the result or print it. 0 = print; 1 = save.")
      val dest = opt[String]("dest", 'o', default = Some("./save/pgsave"),
        descr = "The path to save outputs.")
    }

    val iters = Conf.iters()
    val source = Conf.source()
    val damping = Conf.damping().toDouble
    val partition = Conf.partition()
    val saveOrPrint = Conf.saveOrPrint()
    val dest = Conf.dest()

    val start = System.nanoTime()

    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile(source, partition)
    val links = lines.map{ s =>
      val parts = s.split("\t")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(x => 1.0)
    val hasIncomings = lines.map(s => (s.split("\t")(1), 1.0)).distinct()
    val noIncomings = ranks.subtract(hasIncomings).map(x => (x._1, 1 - damping))cache()

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{
        case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues((1 - damping) + damping * _).union(noIncomings)
    }

    val output = ranks.collect()

    saveOrPrint match {
      case 0 => output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
      case 1 => ranks.saveAsTextFile(dest)
    }

    ctx.stop()
    println("Time took: %f s.".format((System.nanoTime() - start) / 1e9))
  }
}
