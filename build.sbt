name := "LoanStats"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.1.1",
  "mysql" % "mysql-connector-java" % "5.1.16",
  "log4j" % "log4j" % "1.2.17",
  "junit" % "junit" % "4.8.1" % "test",
  "org.scalatest" %% "scalatest" % "2.2.2" % Test


)
