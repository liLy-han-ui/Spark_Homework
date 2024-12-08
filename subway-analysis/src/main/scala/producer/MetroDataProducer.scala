package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.LoggerFactory
import java.util.Properties
import scala.io.Source
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MetroDataProducer {
  private val logger = LoggerFactory.getLogger(getClass)
  private val csvFilePath = "/Users/wangruihan/Downloads/subway-analysis/data/test.csv"
  private val batchInterval = 5000  // 批次间隔（毫秒）
  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting MetroDataProducer with data from: $csvFilePath")

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")

    val producer = new KafkaProducer[String, String](props)
    
    try {
      val source = Source.fromFile(csvFilePath)
      val records = source.getLines().drop(1)  // 跳过表头
      var currentMinute = ""
      var currentBatch = List[String]()
      var recordsSent = 0

      for (record <- records) {
        val timestamp = record.split(",")(0).trim
        val minute = timestamp.substring(0, 16)  // 提取到分钟级别 "yyyy-MM-dd HH:mm"
        
        if (currentMinute.isEmpty) {
          currentMinute = minute
        }
        
        if (minute == currentMinute) {
          // 收集同一分钟的数据
          currentBatch = record :: currentBatch
        } else {
          // 发送上一分钟的数据
          val batchStartTime = System.currentTimeMillis()
          
          // 发送当前批次的数据（保持时间顺序）
          currentBatch.reverse.foreach { line =>
            producer.send(new ProducerRecord[String, String]("subway_data", line))
            recordsSent += 1
          }
          
          producer.flush()
          logger.info(s"Sent batch for minute $currentMinute with ${currentBatch.length} records. " +
            s"Total records sent: $recordsSent")
          
          // 计算并等待到下一个批次
          val processingTime = System.currentTimeMillis() - batchStartTime
          val waitTime = math.max(0, batchInterval - processingTime)
          
          if (waitTime > 0) {
            logger.debug(s"Waiting for ${waitTime}ms before next batch")
            Thread.sleep(waitTime)
          }
          
          // 开始新的一分钟
          currentMinute = minute
          currentBatch = List(record)
        }
      }
      
      // 发送最后一分钟的数据
      if (currentBatch.nonEmpty) {
        currentBatch.reverse.foreach { line =>
          producer.send(new ProducerRecord[String, String]("subway_data", line))
          recordsSent += 1
        }
        producer.flush()
        logger.info(s"Sent final batch for minute $currentMinute with ${currentBatch.length} records. " +
          s"Total records sent: $recordsSent")
      }
      
      source.close()
      logger.info(s"Finished sending all records. Total sent: $recordsSent")
      
    } catch {
      case e: Exception =>
        logger.error("Error in producer", e)
        throw e
    } finally {
      logger.info("Closing producer")
      producer.close()
    }
  }
}
