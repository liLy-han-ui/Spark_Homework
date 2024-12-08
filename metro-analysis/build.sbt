name := "metro-analysis"
version := "1.0"
scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.8.1",
  "org.apache.hbase" % "hbase-client" % "2.4.12",
  "org.apache.hbase" % "hbase-common" % "2.4.12",
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "org.slf4j" % "slf4j-simple" % "1.7.32",
  
  // JFreeChart dependencies
  "org.jfree" % "jfreechart" % "1.5.3",
  "org.jfree" % "jcommon" % "1.0.24"
)