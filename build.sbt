name := "Test"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-actors" % "2.10.4",
    "org.apache.spark" % "spark-streaming_2.10" % "1.3.0",
    "org.rogach" %% "scallop" % "0.9.5",
    "mysql" % "mysql-connector-java" % "5.1.29"
)

mergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
    case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
    case "log4j.properties"                                  => MergeStrategy.discard
    case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
    case "reference.conf"                                    => MergeStrategy.concat
    case _                                                   => MergeStrategy.first
}
