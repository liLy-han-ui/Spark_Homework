import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import org.jfree.chart.ChartFactory
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.category.StackedBarRenderer
import org.jfree.chart.ChartUtilities
import org.jfree.chart.renderer.category.BarRenderer
import org.jfree.data.category.DefaultCategoryDataset
import java.awt.Color
import java.sql.{Connection, DriverManager, PreparedStatement}

object AnalysisFour {
  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val spark = SparkSession.builder()
      .appName("AnalysisFour")
      .getOrCreate()

    import spark.implicits._

    // MySQL 配置
    val jdbcUrl = "jdbc:mysql://localhost:3306/subway_data"
    val dbUser = "root"
    val dbPassword = "******"

    // 保存数据到MySQL
    def saveToMySQL(day: String, lineID: String, passengerFlow: Long): Unit = {
      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        val sql = "REPLACE INTO metro_passenger_flow (day, lineID, passenger_flow) VALUES (?, ?, ?)"
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, s"2019-01-$day")
        preparedStatement.setString(2, lineID)
        preparedStatement.setLong(3, passengerFlow)
        preparedStatement.executeUpdate()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (preparedStatement != null) preparedStatement.close()
        if (connection != null) connection.close()
      }
    }

    // 创建JFreeChart的数据集
    val dataset = new DefaultCategoryDataset

    // 读取25天的数据并统计
    val totalFlows = scala.collection.mutable.Map[String, Long]()
    for (day <- 1 to 25) {
      val dayStr = f"$day%02d"
      val dataPath = s"/formaldata/record_2019-01-$dayStr.csv"
      val metroData = spark.read.option("header", "true")
        .csv(dataPath)
        .toDF("time", "lineID", "stationID", "deviceID", "status", "userID", "payType")

      // 按线路统计每日客流量
      val dailyFlow = metroData.groupBy("lineID").count()
      dailyFlow.collect().foreach(row => {
        val lineID = row.getAs[String]("lineID")
        val count = row.getAs[Long]("count")
        dataset.addValue(count, lineID, s"01-$dayStr")

        // 保存每条线路的客流量到 MySQL
        saveToMySQL(dayStr, lineID, count)
      })

      // 总客流量
      val totalFlow = metroData.count()
      totalFlows(dayStr) = totalFlow

      // 保存总客流量到 MySQL
      saveToMySQL(dayStr, "total", totalFlow)
    }

    // 创建堆叠柱状图
    val chart = ChartFactory.createStackedBarChart(
      "Metro Passenger Flow",
      "Date",
      "Passenger Flow",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      false,
      false
    )

    // 自定义图表样式
    val plot = chart.getPlot.asInstanceOf[CategoryPlot]
    plot.setBackgroundPaint(Color.WHITE) // 设置背景为白色

    // 在柱状图顶端添加总客流量数值标签
    for ((dayStr, totalFlow) <- totalFlows) {
      val annotation = new org.jfree.chart.annotations.CategoryTextAnnotation(
        s"$totalFlow", s"01-$dayStr", totalFlow)
      annotation.setTextAnchor(org.jfree.ui.TextAnchor.BOTTOM_CENTER)
      plot.addAnnotation(annotation)
    }

    // 自定义渲染器
    val renderer = new StackedBarRenderer()
    for (i <- 0 until dataset.getRowCount) {
      renderer.setSeriesItemLabelGenerator(i, new org.jfree.chart.labels.StandardCategoryItemLabelGenerator("{2}", new java.text.DecimalFormat("#")))
      renderer.setSeriesItemLabelsVisible(i, true)
    }
    renderer.setPositiveItemLabelPositionFallback(new org.jfree.chart.labels.ItemLabelPosition(
      org.jfree.chart.labels.ItemLabelAnchor.CENTER, // 标签位于柱子中间
      org.jfree.ui.TextAnchor.CENTER // 标签文本居中显示
    ))
    renderer.setMaximumBarWidth(0.8) // 设置柱子宽度

    plot.setRenderer(renderer)

    // 保存图表为图片
    val outputFile = new File("/home/hadoop/sparkwork/ApplyFive/MetroPassengerFlow.png")
    ChartUtilities.saveChartAsPNG(outputFile, chart, 1600, 900)

    // 停止SparkSession
    spark.stop()
  }
}


 

