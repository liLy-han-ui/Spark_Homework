name := "subway-analysis"
version := "1.0"
scalaVersion := "2.12.15"

// 添加依赖
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0",
  "org.apache.hbase" % "hbase-client" % "2.4.9",
  "org.apache.hbase" % "hbase-common" % "2.4.9",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.jfree" % "jfreechart" % "1.5.3"
)

// 添加运行时配置
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// 添加assembly插件配置
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}