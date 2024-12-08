import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartUtilities
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.data.time.{Hour, Minute, TimeSeries, TimeSeriesCollection}
import org.jfree.chart.axis.{DateAxis, NumberAxis}
import java.text.SimpleDateFormat
import java.awt.{Color, BasicStroke, Shape}
import java.awt.geom.Ellipse2D
import java.sql.{Connection, DriverManager, PreparedStatement}

object AnalysisTwo {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("AnalysisTwo")
      .getOrCreate()

    import spark.implicits._

    // MySQL 配置
    val jdbcUrl = "jdbc:mysql://localhost:3306/subway_data"
    val dbUser = "root"
    val dbPassword = "******"

    // 保存数据到MySQL
    def saveToMySQL(day: String, time: String, lineID: String, status: String, trafficCount: Long): Unit = {
      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        val sql = "REPLACE INTO passenger_flow_by_line (day, time_slot, lineID, status, traffic_count) VALUES (?, ?, ?, ?, ?)"
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, s"2019-01-$day")
        preparedStatement.setString(2, time)
        preparedStatement.setString(3, lineID)
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

    // 定义线路和进出站类型
    val lines = Seq("A", "B", "C")
    val statuses = Seq(("In", 1), ("Out", 0))
    val days = Seq("01", "05", "06", "07", "08", "09", "10", "11")

    days.foreach { day =>
      // 读取数据并计算每半小时的客流量（按线路和进出站类型分开）
      val filePath = s"/formaldata/record_2019-01-$day.csv"
      val data = spark.read.option("header", "true").csv(filePath)
        .toDF("time", "lineID", "stationID", "deviceID", "status", "userID", "payType")
        .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) // 将时间字段解析为 Timestamp 类型
        .withColumn("half_hour", window(col("timestamp"), "30 minutes").getField("start")) // 计算每半小时的窗口时间

      // 创建 TimeSeriesCollection 数据集
      val dataset = new TimeSeriesCollection()

      // 对每条线路和进出站类型进行处理
      lines.foreach { line =>
        statuses.foreach { case (statusLabel, statusValue) =>
          // 过滤出指定线路和进出站类型的数据
          val filteredData = data.filter(col("lineID") === line && col("status") === statusValue)
            .groupBy(date_format(col("half_hour"), "HH:mm").alias("time_only")) // 只保留小时和分钟部分
            .agg(count("*").alias("traffic_count")) // 统计每半小时的客流量
            .orderBy("time_only")
            .collect()

          // 创建时序图数据集
          val timeSeries = new TimeSeries(s"Line $line - $statusLabel")
          filteredData.foreach(row => {
            val timeOnly = row.getAs[String]("time_only")
            val count = row.getAs[Long]("traffic_count")
            val hourMinute = timeOnly.split(":").map(_.toInt)
            val hour = hourMinute(0)
            val minute = hourMinute(1)
            timeSeries.addOrUpdate(new Minute(minute + 15, new Hour(hour, new org.jfree.data.time.Day())), count) // 将时间点移至半小时的中间

            // 保存到 MySQL
            saveToMySQL(day, timeOnly, line, statusLabel, count)
          })
          dataset.addSeries(timeSeries)
        }
      }

      // 创建折线图
      val chart = ChartFactory.createTimeSeriesChart(
        s"Subway Passenger Flow by Line and Status on 2019-01-$day",
        "Time",
        "Passenger Count",
        dataset,
        true,
        false,
        false
      )
      chart.getXYPlot.setBackgroundPaint(Color.WHITE) // 设置背景为白色
      chart.getXYPlot.setDomainGridlinePaint(Color.GRAY) // 设置网格线颜色
      chart.getXYPlot.setRangeGridlinePaint(Color.GRAY) // 设置网格线颜色

      val plot = chart.getXYPlot

      // 自定义渲染和时间轴设置
      val renderer = new XYLineAndShapeRenderer()
      val colors = Array(Color.BLUE, Color.RED, Color.GREEN) // 设置线路的颜色，共三种颜色
      lines.zipWithIndex.foreach { case (line, index) =>
        statuses.foreach { case (statusLabel, statusValue) =>
          val seriesIndex = lines.indexOf(line) * 2 + statuses.indexOf((statusLabel, statusValue))
          renderer.setSeriesPaint(seriesIndex, colors(index)) // 为每条线路的进出站设置相同的颜色
          statusLabel match {
            case "In" =>
              renderer.setSeriesStroke(seriesIndex, new BasicStroke(3.0f)) // 进站使用实线
              renderer.setSeriesShape(seriesIndex, new Ellipse2D.Double(-3.0, -3.0, 6.0, 6.0)) // 进站使用圆形形状
            case "Out" =>
              renderer.setSeriesStroke(seriesIndex, new BasicStroke(3.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 1.0f, Array(5.0f), 0.0f)) // 出站使用虚线
              renderer.setSeriesShape(seriesIndex, new java.awt.Polygon(Array(0, -3, 3), Array(-3, 3, 3), 3)) // 出站使用三角形形状
          }
          renderer.setSeriesShapesVisible(seriesIndex, true) // 显示每个数据点的形状
          renderer.setSeriesShapesFilled(seriesIndex, true)
        }
      }
      plot.setRenderer(renderer)

      val dateAxis = plot.getDomainAxis.asInstanceOf[DateAxis]
      val dateFormat = new SimpleDateFormat("HH:mm")
      dateAxis.setDateFormatOverride(dateFormat) // 设置时间格式为小时和分钟
      dateAxis.setTickUnit(new org.jfree.chart.axis.DateTickUnit(org.jfree.chart.axis.DateTickUnitType.MINUTE, 30)) // 设置每半小时一个间隔

      val rangeAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
      rangeAxis.setRange(0, 55000) // 设置纵坐标的范围为 0 到 55000
      rangeAxis.setTickUnit(new org.jfree.chart.axis.NumberTickUnit(5000)) // 设置纵坐标的刻度间隔为 5000

      // 保存折线图到本地
      val outputFile = new File(s"/home/hadoop/sparkwork/ApplyThree/SubwayTraffic_By_Line_And_Status_2019-01-$day.png")
      ChartUtilities.saveChartAsPNG(outputFile, chart, 1600, 700) // 设置图像宽度为1600，高度为700
    }

    // 停止 SparkSession
    spark.stop()
  }
}


