package consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import java.util.Properties
import java.time.Duration
import scala.collection.mutable
import scala.collection.JavaConverters._
import utils.{ChartGenerator, StationChartGenerator}

object MetroDataConsumer {
  // 状态映射
  private val statusMap = Map(
    "0" -> "出站",
    "1" -> "入站"
  )
  
  // 支付类型映射
  private val payTypeMap = Map(
    "0" -> "普通卡",
    "1" -> "学生卡",
    "2" -> "老年卡",
    "3" -> "其他"
  )

  // HBase配置
  private val HBASE_ZOOKEEPER_QUORUM = "localhost"
  private val LINE_STATS_TABLE = "metro_line_stats"
  private val STATION_STATS_TABLE = "metro_station_stats"
  private val COLUMN_FAMILY = "stats"

  // 定义线路统计数据结构
  case class LineStats(
    inCount: Int = 0,
    outCount: Int = 0,
    totalCount: Int = 0
  )

  // 定义站点统计数据结构
  case class StationStats(
    inCount: Int = 0,
    outCount: Int = 0,
    totalCount: Int = 0
  )

  def main(args: Array[String]): Unit = {
    // Kafka消费者配置
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "metro-analysis-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringDeserializer")

    // 创建Kafka消费者
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Arrays.asList("metro-data"))

    // 初始化图表
    println("Initializing charts...")
    ChartGenerator.initChart()
    StationChartGenerator.initChart()

    // 初始化HBase连接
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    val connection = ConnectionFactory.createConnection(conf)
    val lineStatsTable = connection.getTable(TableName.valueOf(LINE_STATS_TABLE))
    val stationStatsTable = connection.getTable(TableName.valueOf(STATION_STATS_TABLE))

    // 统计数据存储
    var currentMinute = ""
    val lineStats = mutable.Map[String, LineStats]()
    val stationStats = mutable.Map[String, StationStats]()
    
    try {
      println("Starting to consume messages...")
      while (true) {
        val records = consumer.poll(Duration.ofMillis(1000)).asScala
        
        if (records.nonEmpty) {
          records.foreach { record =>
            try {
              val fields = record.value().split(",")
              if (fields.length >= 7) {
                val timeStr = fields(0).trim
                val minute = timeStr.substring(0, 16) // 格式: yyyy-MM-dd HH:mm
                val lineId = fields(1).trim
                val stationId = fields(2).trim
                val status = fields(4).trim

                // 如果是新的一分钟，保存前一分钟的数据并重置统计
                if (currentMinute != "" && currentMinute != minute) {
                  saveLineStats(lineStatsTable, currentMinute, lineStats.toMap)
                  saveStationStats(stationStatsTable, currentMinute, stationStats.toMap)
                  lineStats.clear()
                  stationStats.clear()
                }
                currentMinute = minute

                // 更新线路统计
                val lineStat = lineStats.getOrElseUpdate(lineId, LineStats())
                if (status == "1") { // 入站
                  lineStats(lineId) = lineStat.copy(
                    inCount = lineStat.inCount + 1,
                    totalCount = lineStat.totalCount + 1
                  )
                } else if (status == "0") { // 出站
                  lineStats(lineId) = lineStat.copy(
                    outCount = lineStat.outCount + 1,
                    totalCount = lineStat.totalCount + 1
                  )
                }

                // 更新站点统计
                val stationKey = s"$lineId-$stationId"
                val stationStat = stationStats.getOrElseUpdate(stationKey, StationStats())
                if (status == "1") { // 入站
                  stationStats(stationKey) = stationStat.copy(
                    inCount = stationStat.inCount + 1,
                    totalCount = stationStat.totalCount + 1
                  )
                } else if (status == "0") { // 出站
                  stationStats(stationKey) = stationStat.copy(
                    outCount = stationStat.outCount + 1,
                    totalCount = stationStat.totalCount + 1
                  )
                }
              }
            } catch {
              case e: Exception =>
                println(s"Error processing record: ${e.getMessage}")
                e.printStackTrace()
            }
          }

          // 打印当前统计信息
          println(s"\n统计时间: $currentMinute")
          println("线路统计信息:")
          lineStats.toSeq.sortBy(_._1).foreach { case (line, stats) =>
            println(f"线路 $line: 入站=${stats.inCount}%3d, 出站=${stats.outCount}%3d, 总人数=${stats.totalCount}%3d")
          }
          println("\n站点统计信息:")
          stationStats.toSeq.sortBy(_._1).foreach { case (stationKey, stats) =>
            println(f"站点 $stationKey: 入站=${stats.inCount}%3d, 出站=${stats.outCount}%3d, 总人数=${stats.totalCount}%3d")
          }
          println("-" * 50)
          
          // 更新实时图表
          val lineData = lineStats.map { case (line, stats) => 
            (line, stats.inCount, stats.outCount, stats.totalCount)
          }.toSeq.sortBy(_._1)
          ChartGenerator.updateChart(lineData, currentMinute)

          // 更新站点统计图表
          val stationData = stationStats.map { case (stationKey, stats) =>
            (stationKey, (stats.inCount, stats.outCount, stats.totalCount))
          }.toMap
          StationChartGenerator.updateChart(stationData, currentMinute)
        }

        Thread.sleep(100)
      }
    } catch {
      case e: Exception =>
        println(s"Fatal error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      println("Closing resources...")
      consumer.close()
      lineStatsTable.close()
      stationStatsTable.close()
      connection.close()
    }
  }

  /**
   * 将线路统计数据保存到HBase
   */
  private def saveLineStats(
    table: Table,
    minute: String,
    stats: Map[String, LineStats]
  ): Unit = {
    try {
      stats.foreach { case (lineId, lineStats) =>
        val rowKey = s"$minute-$lineId"
        val put = new Put(Bytes.toBytes(rowKey))
        
        put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY),
          Bytes.toBytes("in_count"),
          Bytes.toBytes(lineStats.inCount.toString)
        )
        put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY),
          Bytes.toBytes("out_count"),
          Bytes.toBytes(lineStats.outCount.toString)
        )
        put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY),
          Bytes.toBytes("total_count"),
          Bytes.toBytes(lineStats.totalCount.toString)
        )
        
        table.put(put)
      }
      println(s"Successfully saved line stats for minute $minute to HBase")
    } catch {
      case e: Exception =>
        println(s"Error saving line stats to HBase: ${e.getMessage}")
        e.printStackTrace()
    }
  }

  /**
   * 将站点统计数据保存到HBase
   */
  private def saveStationStats(
    table: Table,
    minute: String,
    stats: Map[String, StationStats]
  ): Unit = {
    try {
      stats.foreach { case (stationKey, stationStats) =>
        val rowKey = s"$minute-$stationKey"
        val put = new Put(Bytes.toBytes(rowKey))
        
        put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY),
          Bytes.toBytes("in_count"),
          Bytes.toBytes(stationStats.inCount.toString)
        )
        put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY),
          Bytes.toBytes("out_count"),
          Bytes.toBytes(stationStats.outCount.toString)
        )
        put.addColumn(
          Bytes.toBytes(COLUMN_FAMILY),
          Bytes.toBytes("total_count"),
          Bytes.toBytes(stationStats.totalCount.toString)
        )
        
        table.put(put)
      }
      println(s"Successfully saved station stats for minute $minute to HBase")
    } catch {
      case e: Exception =>
        println(s"Error saving station stats to HBase: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}