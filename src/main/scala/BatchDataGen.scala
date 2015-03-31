import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.spark.Logging
import org.rogach.scallop.ScallopConf

import scala.io.Source

/**
 * Created by fangming on 3/21/15.
 */
object BatchDataGen extends Logging {
  //logInfo("aaaa")

  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val in = new BufferedInputStream(new FileInputStream(file))
    val out = fileSystem.create(new Path(file.getName))
    val b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def generateBatchData (src: String, dest: String, size: Long): Unit = {
    val start = System.nanoTime()
    val srcfile = new File(src)
    val is = Source.fromFile(src)
    val out = fileSystem.create(new Path(dest))
    var currSize = "0".toLong
    var counter = 0
    for (line <- is.getLines()) {
      counter += 1
      currSize += line.length
      out.write(line.getBytes())
      out.write("\n".getBytes())
      println("Done: %d / %d".format(currSize, size))
      if (currSize >= size) {
        println("Summary:")
        println("Total size: " + currSize)
        println("Total lines generated: %d.".format(counter))
        println("Total time took %f".format((System.nanoTime() - start) / 1e9))
        out.close()
        is.close()
        return
      }
    }
    println("File generated, time took %f".format((System.nanoTime() - start) / 1e9))
  }

  def main (args: Array[String]) = {
    object Conf extends ScallopConf(args) {
      version("Batch word count")
      val source = opt[String]("source", 'f', default = Some("corpus/whole"),
        descr = "The source file to read from. in Local filesystem")
      val dest = opt[String]("dest", 'd', default = Some("corpus/des"),
        descr = "The path to save outputs, in HDFS.")
      val size = opt[String]("size", 's', default = Some("1000000"),
        descr = "The size of test file.")
    }

    val srcPath = Conf.source()
    val desPath = Conf.dest()
    val size = Conf.size().toLong
    generateBatchData(srcPath, desPath, size)
  }
}
