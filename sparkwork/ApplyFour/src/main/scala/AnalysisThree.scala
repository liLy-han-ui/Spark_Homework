import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import org.jfree.chart.ChartFactory
import org.jfree.chart.plot.PlotOrientation
import org.jfree.data.category.DefaultCategoryDataset
import java.awt.{Color, Font, Paint}
import javax.imageio.ImageIO
import org.jfree.chart.renderer.category.BarRenderer
import java.awt.image.BufferedImage
import org.jfree.chart.JFreeChart
import java.sql.{Connection, DriverManager, PreparedStatement}

object AnalysisThree {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("AnalysisThree")
      .getOrCreate()

    import spark.implicits._

    // MySQL 配置
    val jdbcUrl = "jdbc:mysql://localhost:3306/subway_data"
    val dbUser = "root"
    val dbPassword = "******"

    // 保存数据到MySQL
    def saveToMySQL(day: String, lineID: String, stationID: Int, status: String, trafficCount: Long): Unit = {
      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        val sql = "REPLACE INTO passenger_flow_by_station (day, lineID, stationID, status, traffic_count) VALUES (?, ?, ?, ?, ?)"
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, s"2019-01-$day")
        preparedStatement.setString(2, lineID)
        preparedStatement.setInt(3, stationID)
        preparedStatement.setString(4, status)
        preparedStatement.setInt(5, trafficCount.toInt)
        preparedStatement.executeUpdate()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (preparedStatement != null) preparedStatement.close()
        if (connection != null) connection.close()
      }
    }

    // 定义要处理的日期
    val days = Seq("01", "05", "06", "07", "08", "09", "10", "11")

    // 循环处理每一天的数据
    days.foreach { day =>
      // 读取数据并计算每个站点的进站和出站客流量
      val data = spark.read.option("header", "true").csv(s"/formaldata/record_2019-01-$day.csv")
        .toDF("time", "lineID", "stationID", "deviceID", "status", "userID", "payType")
        .withColumn("stationID", col("stationID").cast("int"))
        .withColumn("status", col("status").cast("int"))

      // 分组统计,并按照 stationID 升序排列
      val trafficCounts = data.groupBy("lineID", "stationID", "status")
        .agg(count("*").alias("count"))
        .orderBy(asc("stationID")) // 按 stationID 升序排列
        .collect()

      // 分别找出进站和出站的前 10 条记录
      val inflowTop10 = trafficCounts.filter(_.getAs[Int]("status") == 1).sortBy(-_.getAs[Long]("count")).take(10).map { row =>
        val lineStation = s"${row.getAs[String]("lineID")}-${row.getAs[Int]("stationID")}"
        lineStation
      }.toSet

      val outflowTop10 = trafficCounts.filter(_.getAs[Int]("status") == 0).sortBy(-_.getAs[Long]("count")).take(10).map { row =>
        val lineStation = s"${row.getAs[String]("lineID")}-${row.getAs[Int]("stationID")}"
        lineStation
      }.toSet

      val inDataset = new DefaultCategoryDataset()
      val outDataset = new DefaultCategoryDataset()
      trafficCounts.foreach { row =>
        val lineStation = s"${row.getAs[String]("lineID")}-${row.getAs[Int]("stationID")}" // 站点名称格式:A-0
        val count = row.getAs[Long]("count")
        val lineID = row.getAs[String]("lineID")
        val stationID = row.getAs[Int]("stationID")
        if (row.getAs[Int]("status") == 1) {
          inDataset.addValue(count, "Station Inflow", lineStation)
          saveToMySQL(day, lineID, stationID, "Inflow", count) // 保存进站数据到 MySQL
        } else {
          outDataset.addValue(count, "Station Outflow", lineStation)
          saveToMySQL(day, lineID, stationID, "Outflow", count) // 保存出站数据到 MySQL
        }
      }

      // 创建进站和出站客流量的横条形图
      def createBarChart(title: String, dataset: DefaultCategoryDataset, primaryColor: Color, secondaryColor: Color, top10: Set[String]): JFreeChart = {
        val chart = ChartFactory.createBarChart(
          title, "Station", "Passenger Flow", dataset, PlotOrientation.HORIZONTAL, true, false, false
        )
        chart.setBackgroundPaint(Color.WHITE)
        val plot = chart.getCategoryPlot
        plot.setBackgroundPaint(Color.WHITE)
        plot.getDomainAxis.setTickLabelFont(new Font("SansSerif", Font.PLAIN, 10))

        // 设置范围轴（数值轴）的最大值为 200000
        val rangeAxis = plot.getRangeAxis
        rangeAxis.setRange(0, 200000)

        val renderer = new BarRenderer() {
          override def getItemPaint(row: Int, column: Int): Paint = {
            val category = dataset.getColumnKey(column).toString
            if (top10.contains(category)) primaryColor else secondaryColor
          }
        }
        plot.setRenderer(renderer)
        chart
      }

      val inChart = createBarChart(s"Station Inflow on 2019-01-$day", inDataset, Color.RED, Color.ORANGE, inflowTop10)
      val outChart = createBarChart(s"Station Outflow on 2019-01-$day", outDataset, Color.BLUE, Color.MAGENTA, outflowTop10)

      // 创建合并后的图像
      val width = 1600
      val height = 900
      val combinedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
      val g2 = combinedImage.createGraphics()
      g2.drawImage(inChart.createBufferedImage(width / 2, height), 0, 0, null)
      g2.drawImage(outChart.createBufferedImage(width / 2, height), width / 2, 0, null)
      g2.dispose()

      // 保存合并后的图表到本地
      ImageIO.write(combinedImage, "png", new File(s"/home/hadoop/sparkwork/ApplyFour/Station_Inflow_Outflow_2019-01-$day.png"))
    }

    // 停止 SparkSession
    spark.stop()
  }
}


