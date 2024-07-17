name := "scoreboard-spark"
version := "1.0"
scalaVersion := "2.12.18"
fork := true

val sparkVersion = "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

Compile / mainClass := Some("scoreboard.BasicScript")
