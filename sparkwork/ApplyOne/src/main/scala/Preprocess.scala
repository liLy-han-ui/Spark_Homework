import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.PrintWriter
import java.io.File

object DataPreprocessing {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("Data Preprocessing")
      .getOrCreate()

    // 禁用生成 _SUCCESS 文件
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    // 创建一个文件来保存结果
    val resultFile = new PrintWriter(new File("/home/hadoop/sparkwork/preprocessing_results.txt"))

    // 读取每个文件并逐个处理
    val inputPath = "hdfs:///workdata/"
    val outputPath = "hdfs:///formaldata/"
    val fileNames = (1 to 25).map(i => f"record_2019-01-${i}%02d.csv")

    // 获取 Hadoop 文件系统对象
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)

    fileNames.foreach { fileName =>
      val df = spark.read.option("header", "true").csv(inputPath + fileName)

      // 返回总记录数
      val totalRecords = df.count()
      resultFile.println(s"File: $fileName")
      resultFile.println(s"Total number of records: $totalRecords")

      // 统计每列有多少个缺失值
      val missingValueCounts = df.select(df.columns.map(c => count(when(col(c).isNull, c)).alias(c)): _*)
      resultFile.println("Missing value counts per column:")
      missingValueCounts.collect().foreach(row => resultFile.println(row))

      // 删除含有缺失值的记录
      var cleanedDf = df.na.drop()

      // 检查并删除重复行
      val duplicateCount = cleanedDf.count() - cleanedDf.dropDuplicates().count()
      resultFile.println(s"Number of duplicate records: $duplicateCount")

      cleanedDf = cleanedDf.dropDuplicates()

      // 检查并删除异常值
      val abnormalConditions = col("time").rlike("^(2019-01-\\d{2} (?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d)$").unary_! ||
        !col("lineID").isin("A", "B", "C") ||
        !col("stationID").between(0, 80) ||
        !col("status").isin(0, 1) ||
        !col("payType").isin(0, 1, 2, 3)

      val abnormalCount = cleanedDf.filter(abnormalConditions).count()
      resultFile.println(s"Number of abnormal records: $abnormalCount")

      // 删除含异常值的记录
      cleanedDf = cleanedDf.filter(!abnormalConditions)

      // 删除时间不在合理范围内的记录
      val timeRangeConditions = col("time") < "2019-01-01 00:00:00" || col("time") > "2019-01-25 23:59:59"
      val timeRangeAbnormalCount = cleanedDf.filter(timeRangeConditions).count()
      resultFile.println(s"Number of records with time out of expected range: $timeRangeAbnormalCount")

      cleanedDf = cleanedDf.filter(!timeRangeConditions)

      // 最终总记录数
      val finalTotalRecords = cleanedDf.count()
      resultFile.println(s"Final total number of records: $finalTotalRecords")

      // 写入临时目录
      val tempOutputPath = outputPath + "temp/"
      cleanedDf.coalesce(1).write.mode("overwrite").option("header", "true").csv(tempOutputPath)

      // 获取生成的 part 文件路径
      val partFile = fs.globStatus(new Path(tempOutputPath + "/part-*"))(0).getPath.getName

      // 重命名文件为原始文件名
      fs.rename(
        new Path(tempOutputPath + "/" + partFile),
        new Path(outputPath + fileName)
      )

      // 删除临时目录
      fs.delete(new Path(tempOutputPath), true)

      // 添加一行空行
      resultFile.println()
    }

    // 关闭文件写入
    resultFile.close()

    // 停止 SparkSession
    spark.stop()
  }
} 

