package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.Properties
import scala.io.Source
import scala.collection.mutable
import java.time.{LocalDateTime, Duration}
import java.time.format.DateTimeFormatter

object MetroDataProducer {
  // 时间缩放因子：1代表实际时间，0.5代表加快一倍，0.1代表加快10倍
  private val TIME_SCALE_FACTOR = 1.0 / 60  // 1分钟压缩为1秒

  def main(args: Array[String]): Unit = {
    // Kafka 生产者配置
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
      "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "metro-data"
    
    try {
      // 读取CSV文件
      val source = Source.fromFile("/Users/wangruihan/Downloads/metro-analysis/data/test.csv")
      
      // 按分钟分组数据
      val dataByMinute = mutable.Map[String, mutable.ArrayBuffer[String]]()
      val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      
      println("Reading and grouping data by minute...")
      
      for (line <- source.getLines.drop(1)) { // 跳过表头
        val minute = line.split(",")(0).trim.substring(0, 16) // 取到分钟
        dataByMinute.getOrElseUpdate(minute, mutable.ArrayBuffer[String]())
          .append(line)
      }
      
      println(s"Grouped data into ${dataByMinute.size} minutes")
      
      // 按时间顺序排序
      val sortedMinutes = dataByMinute.keys.toArray.sorted
      var prevMinute = LocalDateTime.parse(sortedMinutes(0) + ":00", timeFormatter)
      
      println("Starting to send data...")
      
      // 按分钟发送数据
      sortedMinutes.foreach { minute =>
        val currentMinute = LocalDateTime.parse(minute + ":00", timeFormatter)
        
        // 计算需要等待的时间并应用缩放因子
        val waitTime = (Duration.between(prevMinute, currentMinute).toMillis * TIME_SCALE_FACTOR).toLong
        if (waitTime > 0) {
          println(s"Waiting ${waitTime}ms before sending next batch...")
          Thread.sleep(waitTime)
        }
        
        
        // 发送该分钟的所有数据
        val records = dataByMinute(minute)
        println(s"\nSending ${records.size} records for minute $minute")
        
        records.foreach { record =>
          val producerRecord = new ProducerRecord[String, String](topic, record)
          producer.send(producerRecord)
        }
        producer.flush()
        
        prevMinute = currentMinute
        println(s"Sent batch for minute $minute")
      }
      
      println("All data sent successfully")
      
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      producer.close()
    }
  }
}