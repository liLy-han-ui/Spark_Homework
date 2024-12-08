package utils

import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.time.{Second, TimeSeriesCollection, TimeSeries}
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.axis.{DateAxis, NumberAxis}
import java.awt.{Color, BasicStroke, Dimension, Font}
import javax.swing.{JFrame, WindowConstants, JTabbedPane, JPanel}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat

object ChartGenerator {
  private var frame: JFrame = _
  private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
  
  // 懒加载数据集
  private lazy val totalFlowDataset = new TimeSeriesCollection()
  private lazy val inOutFlowDataset = new TimeSeriesCollection()
  
  // 懒加载图表
  private lazy val totalFlowChart = createTotalFlowChart()
  private lazy val inOutFlowChart = createInOutFlowChart()
  
  // 懒加载系列映射
  private lazy val totalSeriesMap = scala.collection.mutable.Map[String, TimeSeries]()
  private lazy val inSeriesMap = scala.collection.mutable.Map[String, TimeSeries]()
  private lazy val outSeriesMap = scala.collection.mutable.Map[String, TimeSeries]()
  
  // 总流量颜色映射
  private val lineColors = Map(
    "A" -> new Color(220, 20, 60),    // 鲜红色
    "B" -> new Color(0, 120, 255),    // 亮蓝色
    "C" -> new Color(0, 180, 0)       // 翠绿色
  )

  // 进出站颜色映射
  private val inOutColors = Map(
    "A-in" -> new Color(220, 20, 60),     // 鲜红色
    "A-out" -> new Color(255, 160, 160),  // 浅粉色
    "B-in" -> new Color(0, 120, 255),     // 亮蓝色
    "B-out" -> new Color(135, 206, 250),  // 天蓝色
    "C-in" -> new Color(0, 180, 0),       // 翠绿色
    "C-out" -> new Color(144, 238, 144)   // 浅绿色
  )

  def initChart(): Unit = {
    // 初始化总流量图表的数据系列
    lineColors.foreach { case (lineId, _) =>
      val totalSeries = new TimeSeries(s"Line $lineId")
      totalFlowDataset.addSeries(totalSeries)
      totalSeriesMap(lineId) = totalSeries

      // 初始化进站和出站数据系列
      val inSeries = new TimeSeries(s"Line $lineId (In)")
      val outSeries = new TimeSeries(s"Line $lineId (Out)")
      inOutFlowDataset.addSeries(inSeries)
      inOutFlowDataset.addSeries(outSeries)
      inSeriesMap(lineId) = inSeries
      outSeriesMap(lineId) = outSeries
    }

    // 创建选项卡面板
    val tabbedPane = new JTabbedPane()
    
    // 创建并添加总流量图表面板
    val totalFlowPanel = createChartPanel(totalFlowChart)
    tabbedPane.addTab("Total Flow", totalFlowPanel)
    
    // 创建并添加进出站流量图表面板
    val inOutFlowPanel = createChartPanel(inOutFlowChart)
    tabbedPane.addTab("In/Out Flow", inOutFlowPanel)

    // 创建并配置主窗口
    frame = new JFrame("Metro Lines Passenger Flow Analysis")
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    frame.setContentPane(tabbedPane)
    frame.pack()
    frame.setLocationRelativeTo(null)
    
    javax.swing.SwingUtilities.invokeLater(() => frame.setVisible(true))
  }

  private def createChartPanel(chart: JFreeChart): JPanel = {
    val panel = new ChartPanel(chart)
    panel.setPreferredSize(new Dimension(1000, 600))
    panel.setMouseWheelEnabled(true)
    panel.setDomainZoomable(true)
    panel.setRangeZoomable(true)
    panel
  }

  private def createTotalFlowChart(): JFreeChart = {
    val chart = ChartFactory.createTimeSeriesChart(
      "Total Passenger Flow",
      "Time",
      "Total Passenger Count",
      totalFlowDataset,
      true,
      true,
      false
    )
    configureChart(chart, totalFlowDataset)
    chart
  }

  private def createInOutFlowChart(): JFreeChart = {
    val chart = ChartFactory.createTimeSeriesChart(
      "In/Out Passenger Flow",
      "Time",
      "Passenger Count",
      inOutFlowDataset,
      true,
      true,
      false
    )
    configureChart(chart, inOutFlowDataset)
    chart
  }

  private def configureChart(chart: JFreeChart, dataset: TimeSeriesCollection): Unit = {
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val renderer = new XYLineAndShapeRenderer(true, false)  // 移除数据点标记
    
    // 配置时间轴
    val dateAxis = plot.getDomainAxis.asInstanceOf[DateAxis]
    dateAxis.setDateFormatOverride(new SimpleDateFormat("HH:mm"))
    dateAxis.setLabelFont(new Font("Arial", Font.BOLD, 14))
    dateAxis.setTickLabelFont(new Font("Arial", Font.PLAIN, 12))
    
    // 配置数值轴
    val numberAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    numberAxis.setLabelFont(new Font("Arial", Font.BOLD, 14))
    numberAxis.setTickLabelFont(new Font("Arial", Font.PLAIN, 12))
    numberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits())
    
    // 设置线条样式 - 使用圆角连接使线条更平滑
    val stroke = new BasicStroke(
      2.5f,                     // 线条宽度
      BasicStroke.CAP_ROUND,    // 圆形线帽
      BasicStroke.JOIN_ROUND    // 圆形连接
    )
    
    // 根据不同图表类型设置不同的颜色方案
    if (dataset == totalFlowDataset) {
      // 总流量图表的颜色设置
      for (i <- 0 until dataset.getSeriesCount) {
        val seriesKey = dataset.getSeriesKey(i).toString
        val lineId = seriesKey.split(" ")(1)
        val color = lineColors.getOrElse(lineId, Color.BLACK)
        
        renderer.setSeriesStroke(i, stroke)
        renderer.setSeriesPaint(i, color)
      }
    } else {
      // 进出站流量图表的颜色设置
      for (i <- 0 until dataset.getSeriesCount) {
        val seriesKey = dataset.getSeriesKey(i).toString
        val lineId = seriesKey.split(" ")(1)
        val isIn = seriesKey.contains("(In)")
        val colorKey = s"$lineId-${if (isIn) "in" else "out"}"
        val color = inOutColors.getOrElse(colorKey, Color.BLACK)
        
        renderer.setSeriesStroke(i, stroke)
        renderer.setSeriesPaint(i, color)
      }
    }
    
    // 设置图表外观
    plot.setRenderer(renderer)
    plot.setBackgroundPaint(Color.WHITE)
    plot.setDomainGridlinePaint(new Color(220, 220, 220))  // 更浅的网格线
    plot.setRangeGridlinePaint(new Color(220, 220, 220))
    
    // 设置图例
    chart.getLegend.setItemFont(new Font("Arial", Font.BOLD, 12))
    chart.getTitle.setFont(new Font("Arial", Font.BOLD, 16))
    
    // 设置绘图区域的内边距
    plot.setInsets(new org.jfree.chart.ui.RectangleInsets(5, 5, 5, 5))
  }

  def updateChart(lineData: Seq[(String, Int, Int, Int)], timeStr: String): Unit = {
    try {
      javax.swing.SwingUtilities.invokeLater(() => {
        try {
          val currentTime = LocalDateTime.parse(timeStr, timeFormatter)
          val second = new Second(
            0,
            currentTime.getMinute,
            currentTime.getHour,
            currentTime.getDayOfMonth,
            currentTime.getMonthValue,
            currentTime.getYear
          )
          
          // 更新两个图表的数据
          lineData.foreach { case (lineId, inCount, outCount, totalCount) =>
            // 更新总流量图表
            totalSeriesMap.get(lineId).foreach(_.addOrUpdate(second, totalCount))
            
            // 更新进出站流量图表
            inSeriesMap.get(lineId).foreach(_.addOrUpdate(second, inCount))
            outSeriesMap.get(lineId).foreach(_.addOrUpdate(second, outCount))
          }
          
          // 打印调试信息
          println(s"\nUpdated charts at $timeStr")
          lineData.foreach { case (lineId, in, out, total) =>
            println(f"Line $lineId: in=$in%d, out=$out%d, total=$total%d")
          }
        } catch {
          case e: Exception =>
            println(s"Error updating charts: ${e.getMessage}")
            e.printStackTrace()
        }
      })
    } catch {
      case e: Exception =>
        println(s"Error in chart update: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}