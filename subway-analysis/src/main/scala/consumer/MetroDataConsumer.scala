package consumer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory
import visualization.MetroChartViewer
import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.collection.mutable
import org.apache.spark.sql.streaming.StreamingQueryException

case class SubwayRecord(
  time: String,
  lineID: String,
  stationID: Int,
  deviceID: Int,
  status: Int,
  userID: String,
  payType: Int
)

object MetroDataConsumer {
  private val logger = LoggerFactory.getLogger(getClass)
  private val chartViewer = new MetroChartViewer()
  
  def main(args: Array[String]): Unit = {
    logger.info("Starting MetroDataConsumer...")
    
    chartViewer.setVisible(true)

    val spark = SparkSession.builder()
      .appName("SubwayDataProcessing")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", "checkpoint")
      .config("spark.sql.streaming.minBatchesToRetain", "100")
      .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "10")
      .getOrCreate()

    import spark.implicits._

    try {
      // 从Kafka读取数据
      val inputDF = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "subway_data")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()

      logger.info("Kafka source configured")

      // 解析数据
      val recordsDF = inputDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(line => {
          try {
            val fields = line.split(",")
            if (fields.length == 7) {
              Some(SubwayRecord(
                fields(0).trim,
                fields(1).trim,
                fields(2).trim.toInt,
                fields(3).trim.toInt,
                fields(4).trim.toInt,
                fields(5).trim,
                fields(6).trim.toInt
              ))
            } else {
              logger.error(s"Invalid record format: $line")
              None
            }
          } catch {
            case e: Exception =>
              logger.error(s"Error parsing record: $line", e)
              None
          }
        })
        .filter(_.isDefined)
        .map(_.get)

      // 按线路统计
      val lineStats = recordsDF
        .withColumn("minute", date_trunc("minute", to_timestamp($"time")))
        .withWatermark("minute", "1 minutes")
        .groupBy(window($"minute", "1 minute"), $"lineID")
        .agg(
          count("*").as("total_flow"),
          sum(when($"status" === 0, 1).otherwise(0)).as("exit_count"),
          sum(when($"status" === 1, 1).otherwise(0)).as("entry_count")
        )
        .select(
          $"window.start".as("minute"),
          $"lineID",
          $"total_flow",
          $"exit_count",
          $"entry_count"
        )

      // 按站点统计
      val stationStats = recordsDF
        .withColumn("minute", date_trunc("minute", to_timestamp($"time")))
        .withWatermark("minute", "1 minutes")
        .groupBy(window($"minute", "1 minute"), $"lineID", $"stationID")
        .agg(
          count("*").as("total_flow"),
          sum(when($"status" === 0, 1).otherwise(0)).as("exit_count"),
          sum(when($"status" === 1, 1).otherwise(0)).as("entry_count")
        )
        .select(
          $"window.start".as("minute"),
          $"lineID",
          $"stationID",
          $"total_flow",
          $"exit_count",
          $"entry_count"
        )

      // 写入线路统计数据并更新图表
      val lineQuery = lineStats.writeStream
        .outputMode("update")
        .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
          val count = batchDF.count()
          if (count > 0) {
            logger.info(s"Processing batch $batchId with $count records")
            batchDF.persist()
            val connection = getHBaseConnection()
            
            try {
              val table = connection.getTable(TableName.valueOf("line_statistics"))
              
              batchDF.collect().foreach { row =>
                val timestamp = row.getAs[java.sql.Timestamp]("minute")
                val lineID = row.getAs[String]("lineID")
                val totalFlow = row.getAs[Long]("total_flow")
                val exitCount = row.getAs[Long]("exit_count")
                val entryCount = row.getAs[Long]("entry_count")

                // 写入HBase
                val rowKey = s"${timestamp.toString}_$lineID"
                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("time"),
                  Bytes.toBytes(timestamp.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("line_id"),
                  Bytes.toBytes(lineID))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("total_flow"),
                  Bytes.toBytes(totalFlow.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exit_count"),
                  Bytes.toBytes(exitCount.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entry_count"),
                  Bytes.toBytes(entryCount.toString))
                
                table.put(put)

                // 更新图表
                chartViewer.updateData(lineID, timestamp, totalFlow, entryCount, exitCount)
              }
              
              table.close()
            } catch {
              case e: Exception =>
                logger.error("Error processing batch", e)
                throw e
            } finally {
              connection.close()
              batchDF.unpersist()
            }
          }
        }
        .trigger(Trigger.ProcessingTime("0 seconds"))
        .start()

      // 写入站点统计数据
      val stationQuery = stationStats.writeStream
        .outputMode("update")
        .foreachBatch { (batchDF: Dataset[Row], batchId: Long) =>
          val count = batchDF.count()
          if (count > 0) {
            logger.info(s"Processing station stats batch $batchId with $count records")
            batchDF.persist()
            val connection = getHBaseConnection()
            
            try {
              val table = connection.getTable(TableName.valueOf("station_statistics"))
              
              batchDF.collect().foreach { row =>
                val timestamp = row.getAs[java.sql.Timestamp]("minute")
                val lineID = row.getAs[String]("lineID")
                val stationID = row.getAs[Int]("stationID")
                val totalFlow = row.getAs[Long]("total_flow")
                val exitCount = row.getAs[Long]("exit_count")
                val entryCount = row.getAs[Long]("entry_count")

                val rowKey = s"${timestamp.toString}_${lineID}_$stationID"
                val put = new Put(Bytes.toBytes(rowKey))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("time"),
                  Bytes.toBytes(timestamp.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("line_id"),
                  Bytes.toBytes(lineID))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("station_id"),
                  Bytes.toBytes(stationID.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("total_flow"),
                  Bytes.toBytes(totalFlow.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exit_count"),
                  Bytes.toBytes(exitCount.toString))
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entry_count"),
                  Bytes.toBytes(entryCount.toString))
                
                table.put(put)
              }
              
              table.close()
            } catch {
              case e: Exception =>
                logger.error("Error processing station stats batch", e)
                throw e
            } finally {
              connection.close()
              batchDF.unpersist()
            }
          }
        }
        .trigger(Trigger.ProcessingTime("0 seconds"))
        .start()

      logger.info("Stream processing started")

      // 等待查询终止
      try {
        lineQuery.awaitTermination()
        stationQuery.awaitTermination()
      } catch {
        case e: StreamingQueryException =>
          logger.error("Streaming query terminated with error", e)
          throw e
      }
      
    } catch {
      case e: Exception =>
        logger.error("Fatal error in consumer", e)
        throw e
    } finally {
      logger.info("Shutting down consumer")
      spark.stop()
    }
  }

  private def getHBaseConnection(): Connection = {
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "localhost")
    config.set("hbase.zookeeper.property.clientPort", "2181")
    ConnectionFactory.createConnection(config)
  }
}