import org.rogach.scallop.{ScallopConf, ScallopOption}

object Conf extends ScallopConf(List("-c","3","-E","fruit=apple","7.2")) {
  // all options that are applicable to builder (like description, default, etc)
  // are applicable here as well
  val count:ScallopOption[Int] = opt[Int]("count", descr = "count the trees", required = true)
    .map(1+) // also here work all standard Option methods -
  val properties = props[String]('E')
  val size:ScallopOption[Double] = trailArg[Double](required = false)
  val x = opt[String]("x")
}


val x = Conf

