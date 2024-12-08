import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import org.jfree.chart.ChartFactory
import org.jfree.chart.plot.PiePlot
import org.jfree.chart.ChartUtilities
import org.jfree.data.general.DefaultPieDataset
import java.awt.Color
import java.sql.{Connection, DriverManager, PreparedStatement}

object AnalysisFive {
  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val spark = SparkSession.builder()
      .appName("AnalysisFive")
      .getOrCreate()

    import spark.implicits._

    // MySQL 配置
    val jdbcUrl = "jdbc:mysql://localhost:3306/subway_data"
    val dbUser = "root"
    val dbPassword = "******"

    // 保存数据到MySQL
    def saveToMySQL(payType: String, totalCount: Long): Unit = {
      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        val sql = "REPLACE INTO paytype (pay_type, total_count) VALUES (?, ?)"
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, payType)
        preparedStatement.setLong(2, totalCount)
        preparedStatement.executeUpdate()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (preparedStatement != null) preparedStatement.close()
        if (connection != null) connection.close()
      }
    }

    // 创建一个DataFrame来存储25天的数据
    val payTypeCounts = scala.collection.mutable.Map[String, Long]()
    for (day <- 1 to 25) {
      val dayStr = f"$day%02d"
      val dataPath = s"/formaldata/record_2019-01-$dayStr.csv"
      val metroData = spark.read.option("header", "true")
        .csv(dataPath)
        .toDF("time", "lineID", "stationID", "deviceID", "status", "userID", "payType")

      // 按支付方式统计
      val dailyPayTypeCount = metroData.groupBy("payType").count()
      dailyPayTypeCount.collect().foreach { row =>
        val payType = row.getAs[String]("payType")
        val count = row.getAs[Long]("count")
        payTypeCounts(payType) = payTypeCounts.getOrElse(payType, 0L) + count
      }
    }

    // 保存到 MySQL
    payTypeCounts.foreach { case (payType, totalCount) =>
      saveToMySQL(payType, totalCount)
    }

    // 创建饼状图的数据集
    val dataset = new DefaultPieDataset
    payTypeCounts.foreach { case (payType, count) =>
      dataset.setValue(s"$payType", count)
    }

    // 创建饼状图
    val chart = ChartFactory.createPieChart(
      "PayType Distribution",
      dataset,
      true,
      false,
      false
    )

    // 自定义饼状图样式
    val plot = chart.getPlot.asInstanceOf[PiePlot]
    dataset.getKeys.forEach { key =>
      plot.setExplodePercent(key.toString, 0.1) // 将每个块都离中心往外移动一点
    }
    plot.setBackgroundPaint(Color.WHITE) // 设置背景为白色
    plot.setCircular(true) // 设置为圆形
    plot.setLabelGenerator(new org.jfree.chart.labels.StandardPieSectionLabelGenerator("{0}: {2}")) // 支付方式和百分比一起显示

    // 保存饼状图为图片
    val outputFile = new File("/home/hadoop/sparkwork/ApplySix/PayType.png")
    ChartUtilities.saveChartAsPNG(outputFile, chart, 800, 800)

    // 停止SparkSession
    spark.stop()
  }
}
