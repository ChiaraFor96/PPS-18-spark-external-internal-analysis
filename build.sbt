name := "PPS-18-spark-external-internal-analysis"

version := "0.1"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4")

libraryDependencies += "log4j" % "log4j" % "1.2.17"

