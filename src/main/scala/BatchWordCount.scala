import java.io._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop.ScallopConf
import scala.io.Source

/**
 * Created by fangming on 3/10/15.
 */
object BatchWordCount {

  def generateInput (src: String, dest: String, size: Long): Unit = {
    val start = System.nanoTime()
    val is = Source.fromFile(src)
    val out = new PrintWriter(new File(dest))
    var currSize = "0".toLong
    for (line <- is.getLines()) {
      currSize += line.length.toLong
      out.println(line)
      if (currSize >= size) return
    }
    println("File generated, time took %f".format((System.nanoTime() - start) / 1e9))
  }

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("Batch word count")
      val source = opt[String]("source", 'f', default = Some("corpus/incre"),
        descr = "The source file to read from.")
      val resultPath = opt[String]("resultPath", 'o', default = Some("corpus/result"),
        descr = "The path to save result.")
      val memLevel = opt[String]("memLevel", 'm', default = Some("default"),
        descr = "The persistence level of DStream.")
    }

    val memLevel = Conf.memLevel() match {
      case "default" => StorageLevel.MEMORY_ONLY_SER
      case x => StorageLevel.fromString(x)
    }
    val resPath = Conf.resultPath()


    val Separator = 31.toChar.toString
    val srcPath = Conf.source()
    var curSize = "0".toLong

    val sparkConf = new SparkConf()
      .setAppName("BatchWordCount")

    val start = System.nanoTime()
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile(srcPath)

    val counts = textFile
      .map(x => x.split(Separator))
      .map(x => (x(0), x(1)
      .split(" ")
      .map(x => (x, 1))
      .groupBy(_._1)
      .map { case (word: String, traversable) => traversable.reduce{(a,b) => (a._1, a._2 + b._2)}}))

    counts.saveAsTextFile(resPath)
    sc.stop()
    println("Test finished, time took: %f".format((System.nanoTime() - start) / 1e9))
  }
}
