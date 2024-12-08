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
import java.awt.Color
import java.awt.BasicStroke
import java.sql.{Connection, DriverManager, PreparedStatement}

object AnalysisOne {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("Analysis One")
      .getOrCreate()

    import spark.implicits._

    // MySQL 配置
    val jdbcUrl = "jdbc:mysql://localhost:3306/subway_data"
    val dbUser = "root"
    val dbPassword = "******"

    // 保存数据到MySQL
    def saveToMySQL(day: String, time: String, trafficCount: Long): Unit = {
      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword)
        val sql = "REPLACE INTO passenger_flow (day, time_slot, traffic_count) VALUES (?, ?, ?)"
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, s"2019-01-$day")
        preparedStatement.setString(2, time)
        preparedStatement.setInt(3, trafficCount.toInt)
        preparedStatement.executeUpdate()
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (preparedStatement != null) preparedStatement.close()
        if (connection != null) connection.close()
      }
    }

    // 定义时间段及其对应的数据集
    val dateGroups = Seq(
      ("PassengerFlow_05_to_11.png", Seq("05", "06", "07", "08", "09", "10", "11")),
      ("PassengerFlow_12_to_18.png", Seq("12", "13", "14", "15", "16", "17", "18")),
      ("PassengerFlow_19_to_25.png", Seq("19", "20", "21", "22", "23", "24", "25"))
    )

    // 单独处理并保存1月1日的数据
    val baseFilePath = "/formaldata/record_2019-01-01.csv"
    val baseData = spark.read.option("header", "true").csv(baseFilePath)
      .toDF("time", "lineID", "stationID", "deviceID", "status", "userID", "payType")
      .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) // 将时间字段解析为 Timestamp 类型
      .withColumn("half_hour", window(col("timestamp"), "30 minutes").getField("start")) // 计算每半小时的窗口时间
      .groupBy(date_format(col("half_hour"), "HH:mm").alias("time_only")) // 只保留小时和分钟部分
      .agg(count("*").alias("traffic_count")) // 统计每半小时的客流量
      .orderBy("time_only")
      .collect()

    val baseTimeSeries = new TimeSeries("01-01")
    baseData.foreach(row => {
      val timeOnly = row.getAs[String]("time_only")
      val count = row.getAs[Long]("traffic_count")
      val hourMinute = timeOnly.split(":").map(_.toInt)
      val hour = hourMinute(0)
      val minute = hourMinute(1)
      baseTimeSeries.addOrUpdate(new Minute(minute + 15, new Hour(hour, new org.jfree.data.time.Day())), count) // 将时间点移至半小时的中间

      saveToMySQL("01", timeOnly, count)
    })

    // 可视化数据
    dateGroups.foreach { case (outputFileName, days) =>
      val dataset = new TimeSeriesCollection()
      // 将1月1日的数据加入到图表数据集中
      dataset.addSeries(baseTimeSeries)

      // 处理指定日期的数据文件
      days.foreach { day =>
        val filePath = s"/formaldata/record_2019-01-$day.csv"
        val data = spark.read.option("header", "true").csv(filePath)
          .toDF("time", "lineID", "stationID", "deviceID", "status", "userID", "payType")
          .withColumn("timestamp", to_timestamp(col("time"), "yyyy-MM-dd HH:mm:ss")) // 将时间字段解析为 Timestamp 类型
          .withColumn("half_hour", window(col("timestamp"), "30 minutes").getField("start")) // 计算每半小时的窗口时间
          .groupBy(date_format(col("half_hour"), "HH:mm").alias("time_only")) // 只保留小时和分钟部分
          .agg(count("*").alias("traffic_count")) // 统计每半小时的客流量
          .orderBy("time_only")
          .collect()

        val timeSeries = new TimeSeries(s"01-$day")
        data.foreach(row => {
          val timeOnly = row.getAs[String]("time_only")
          val count = row.getAs[Long]("traffic_count")
          val hourMinute = timeOnly.split(":").map(_.toInt)
          val hour = hourMinute(0)
          val minute = hourMinute(1)
          timeSeries.addOrUpdate(new Minute(minute + 15, new Hour(hour, new org.jfree.data.time.Day())), count) // 将时间点移至半小时的中间

          saveToMySQL(day, timeOnly, count)
        })
        dataset.addSeries(timeSeries) // 将当前天的数据加入到图表数据集中
      }

      // 创建折线图
      val chart = ChartFactory.createTimeSeriesChart(
        s"Subway Passenger Flow from 2019-01-${days.head} to 2019-01-${days.last}",
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
      val colors = Array(Color.BLUE, Color.RED, Color.GREEN, Color.MAGENTA, Color.ORANGE, Color.CYAN, Color.PINK, Color.YELLOW) // 设置折线的颜色，区分度更高
      for (i <- 0 until dataset.getSeriesCount) {
        renderer.setSeriesStroke(i, new BasicStroke(3.0f)) // 设置每条折线的粗细
        renderer.setSeriesShapesVisible(i, true) // 显示每个数据点的形状
        renderer.setSeriesShapesFilled(i, true)
        renderer.setSeriesPaint(i, colors(i)) // 设置每条折线的颜色
      }
      plot.setRenderer(renderer)

      val dateAxis = plot.getDomainAxis.asInstanceOf[DateAxis]
      val dateFormat = new SimpleDateFormat("HH:mm")
      dateAxis.setDateFormatOverride(dateFormat) // 设置时间格式为小时和分钟
      dateAxis.setTickUnit(new org.jfree.chart.axis.DateTickUnit(org.jfree.chart.axis.DateTickUnitType.MINUTE, 30)) // 设置每半小时一个间隔

      val rangeAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
      rangeAxis.setRange(0, 200000) // 设置纵坐标的范围为 0 到 200000
      rangeAxis.setTickUnit(new org.jfree.chart.axis.NumberTickUnit(10000)) // 设置纵坐标的刻度间隔为 10000

      // 保存折线图到本地
      val outputFile = new File(s"/home/hadoop/sparkwork/ApplyTwo/$outputFileName")
      ChartUtilities.saveChartAsPNG(outputFile, chart, 1600, 700) // 设置图像宽度为1600，高度为700
    }

    // 停止 SparkSession
    spark.stop()
  }
}


