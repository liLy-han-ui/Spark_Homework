package visualization

import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.data.time.{Minute, TimeSeriesCollection, TimeSeries}
import org.jfree.chart.axis.{DateAxis, NumberAxis, ValueAxis}
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import javax.swing.{JFrame, JPanel, JTabbedPane, BorderFactory, WindowConstants, JButton, BoxLayout}
import java.awt.{BorderLayout, Color, Dimension, BasicStroke, Font, FlowLayout}
import java.util.Date
import java.text.SimpleDateFormat
import org.slf4j.LoggerFactory
import javax.swing.SwingUtilities
import java.awt.event.ActionListener
import java.awt.event.ActionEvent

class MetroChartViewer extends JFrame("Metro Traffic Real-time Visualization") {
  private val logger = LoggerFactory.getLogger(getClass)

  // 为每条线路创建时间序列，不设置最大数据点限制
  private val totalFlowSeries = Map(
    "A" -> new TimeSeries("Line A"),
    "B" -> new TimeSeries("Line B"),
    "C" -> new TimeSeries("Line C")
  )
  
  private val entrySeries = Map(
    "A" -> new TimeSeries("Line A Entry"),
    "B" -> new TimeSeries("Line B Entry"),
    "C" -> new TimeSeries("Line C Entry")
  )
  
  private val exitSeries = Map(
    "A" -> new TimeSeries("Line A Exit"),
    "B" -> new TimeSeries("Line B Exit"),
    "C" -> new TimeSeries("Line C Exit")
  )

  // 创建数据集
  private val totalFlowDataset = new TimeSeriesCollection()
  private val inOutDataset = new TimeSeriesCollection()

  // 初始化数据集
  totalFlowSeries.values.foreach(totalFlowDataset.addSeries)
  entrySeries.values.foreach(inOutDataset.addSeries)
  exitSeries.values.foreach(inOutDataset.addSeries)

  private def createTotalFlowChart(): JFreeChart = {
    val chart = ChartFactory.createTimeSeriesChart(
      "Total Passenger Flow by Line",  // 标题
      "Time",                          // x轴标签
      "Number of Passengers",          // y轴标签
      totalFlowDataset,               // 数据集
      true,                           // 显示图例
      true,                           // 显示工具提示
      false                           // 不显示URLs
    )

    customizeChart(chart)
    customizeTotalFlowChart(chart)
    chart
  }

  private def createInOutChart(): JFreeChart = {
    val chart = ChartFactory.createTimeSeriesChart(
      "Entry/Exit Passenger Flow by Line",
      "Time",
      "Number of Passengers",
      inOutDataset,
      true,
      true,
      false
    )

    customizeChart(chart)
    customizeInOutChart(chart)
    chart
  }

  // 创建图表
  private val totalFlowChart = createTotalFlowChart()
  private val inOutChart = createInOutChart()

  // 创建图表面板
  private val totalFlowPanel = createChartPanel(totalFlowChart)
  private val inOutPanel = createChartPanel(inOutChart)

  // 创建控制面板
  private val controlPanel = new JPanel(new FlowLayout(FlowLayout.CENTER))
  private val tabbedPane = new JTabbedPane()

  // 自动滚动标志
  private var autoScroll = true

  // 初始化UI
  initializeUI()

  private def initializeUI(): Unit = {
    setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    setLayout(new BorderLayout())
    
    // 设置面板大小
    val preferredSize = new Dimension(1200, 800)
    totalFlowPanel.setPreferredSize(preferredSize)
    inOutPanel.setPreferredSize(preferredSize)

    // 创建控制按钮
    val zoomInButton = new JButton("放大")
    val zoomOutButton = new JButton("缩小")
    val resetButton = new JButton("重置")
    val autoScrollButton = new JButton("自动滚动: 开")

    // 设置按钮字体
    val buttonFont = new Font("SansSerif", Font.PLAIN, 12)
    zoomInButton.setFont(buttonFont)
    zoomOutButton.setFont(buttonFont)
    resetButton.setFont(buttonFont)
    autoScrollButton.setFont(buttonFont)

    // 添加按钮事件
    zoomInButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent): Unit = {
        zoomCharts(0.5)
      }
    })

    zoomOutButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent): Unit = {
        zoomCharts(2.0)
      }
    })

    resetButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent): Unit = {
        resetCharts()
      }
    })

    autoScrollButton.addActionListener(new ActionListener {
      def actionPerformed(e: ActionEvent): Unit = {
        autoScroll = !autoScroll
        autoScrollButton.setText(s"自动滚动: ${if (autoScroll) "开" else "关"}")
      }
    })

    // 添加按钮到控制面板
    controlPanel.add(zoomInButton)
    controlPanel.add(zoomOutButton)
    controlPanel.add(resetButton)
    controlPanel.add(autoScrollButton)

    // 设置控制面板的首选大小
    controlPanel.setPreferredSize(new Dimension(1200, 40))

    // 添加选项卡
    tabbedPane.addTab("总客流量", totalFlowPanel)
    tabbedPane.addTab("进出站流量", inOutPanel)
    tabbedPane.setFont(new Font("SansSerif", Font.PLAIN, 14))
    
    // 添加面板到框架
    add(controlPanel, BorderLayout.NORTH)
    add(tabbedPane, BorderLayout.CENTER)

    pack()
    setLocationRelativeTo(null)  // 居中显示
  }

  private def createChartPanel(chart: JFreeChart): ChartPanel = {
    val panel = new ChartPanel(chart) {
      setMouseWheelEnabled(true)
      setZoomInFactor(0.8)
      setZoomOutFactor(1.2)
      setMouseZoomable(true)
    }
    
    // 启用拖动和缩放
    panel.setDomainZoomable(true)
    panel.setRangeZoomable(true)
    panel.setMouseWheelEnabled(true)
    
    panel.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10))
    panel
  }

  private def customizeChart(chart: JFreeChart): Unit = {
    // 设置背景
    chart.setBackgroundPaint(Color.WHITE)
    
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    
    // 设置绘图区域
    plot.setBackgroundPaint(new Color(255, 255, 255))
    plot.setDomainGridlinePaint(new Color(220, 220, 220))
    plot.setRangeGridlinePaint(new Color(220, 220, 220))
    plot.setOutlinePaint(Color.GRAY)
    plot.setDomainGridlinesVisible(true)
    plot.setRangeGridlinesVisible(true)
    
    // 设置日期轴
    val dateAxis = plot.getDomainAxis.asInstanceOf[DateAxis]
    dateAxis.setDateFormatOverride(new SimpleDateFormat("HH:mm"))
    dateAxis.setLabelFont(new Font("SansSerif", Font.BOLD, 14))
    dateAxis.setTickLabelFont(new Font("SansSerif", Font.PLAIN, 12))
    
    // 设置数值轴
    val rangeAxis = plot.getRangeAxis
    rangeAxis.setLabelFont(new Font("SansSerif", Font.BOLD, 14))
    rangeAxis.setTickLabelFont(new Font("SansSerif", Font.PLAIN, 12))
    
    // 设置图例和标题
    chart.getLegend.setItemFont(new Font("SansSerif", Font.PLAIN, 12))
    chart.getTitle.setFont(new Font("SansSerif", Font.BOLD, 16))
  }

  private def customizeTotalFlowChart(chart: JFreeChart): Unit = {
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val renderer = plot.getRenderer
    
    // 设置线条颜色和粗细
    renderer.setSeriesPaint(0, new Color(255, 99, 71))   // Line A - 红色
    renderer.setSeriesPaint(1, new Color(65, 105, 225))  // Line B - 蓝色
    renderer.setSeriesPaint(2, new Color(50, 205, 50))   // Line C - 绿色
    
    for (i <- 0 until 3) {
      renderer.setSeriesStroke(i, new BasicStroke(2.5f))
    }
  }

  private def customizeInOutChart(chart: JFreeChart): Unit = {
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val renderer = new XYLineAndShapeRenderer()
    plot.setRenderer(renderer)
    
    // 设置入站和出站的不同样式
    val colors = Array(
      new Color(255, 99, 71),   // Line A
      new Color(65, 105, 225),  // Line B
      new Color(50, 205, 50)    // Line C
    )
    
    for (i <- 0 until 3) {
      // 入站 - 实线
      renderer.setSeriesPaint(i, colors(i))
      renderer.setSeriesStroke(i, new BasicStroke(2.5f))
      
      // 出站 - 虚线
      renderer.setSeriesPaint(i + 3, colors(i))
      renderer.setSeriesStroke(i + 3,
        new BasicStroke(2.0f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER,
          10.0f, Array(10.0f), 0.0f))
    }
  }

  private def zoomCharts(factor: Double): Unit = {
    val charts = Array(totalFlowChart, inOutChart)
    charts.foreach { chart =>
      val plot = chart.getPlot.asInstanceOf[XYPlot]
      val domainAxis = plot.getDomainAxis
      val rangeAxis = plot.getRangeAxis
      
      val domainRange = domainAxis.getRange
      val rangeRange = rangeAxis.getRange
      
      domainAxis.setRange(domainRange.getLowerBound,
        domainRange.getLowerBound + (domainRange.getLength * factor))
      rangeAxis.setRange(rangeRange.getLowerBound,
        rangeRange.getLowerBound + (rangeRange.getLength * factor))
    }
  }

  private def resetCharts(): Unit = {
    val charts = Array(totalFlowChart, inOutChart)
    charts.foreach { chart =>
      val plot = chart.getPlot.asInstanceOf[XYPlot]
      plot.getDomainAxis.setAutoRange(true)
      plot.getRangeAxis.setAutoRange(true)
    }
  }

  def updateData(lineID: String, minute: Date, totalFlow: Long, entryCount: Long, exitCount: Long): Unit = {
    try {
      SwingUtilities.invokeLater(new Runnable {
        def run(): Unit = {
          if (totalFlowSeries.contains(lineID)) {
            val minuteObj = new Minute(minute)
            
            // 更新总流量
            totalFlowSeries(lineID).addOrUpdate(minuteObj, totalFlow)
            
            // 更新入站和出站数据
            entrySeries(lineID).addOrUpdate(minuteObj, entryCount)
            exitSeries(lineID).addOrUpdate(minuteObj, exitCount)
            
            // 只在自动滚动模式下自动调整轴范围
            if (autoScroll) {
              val charts = Array(totalFlowChart, inOutChart)
              charts.foreach { chart =>
                val plot = chart.getPlot.asInstanceOf[XYPlot]
                plot.getDomainAxis.setAutoRange(true)
                plot.getRangeAxis.setAutoRange(true)
              }
            }
            
            logger.debug(s"Updated chart data: line=$lineID, time=${minute}, " +
              s"total=$totalFlow, entry=$entryCount, exit=$exitCount")
          }
        }
      })
    } catch {
      case e: Exception =>
        logger.error(s"Error updating chart data for line $lineID", e)
    }
  }
}
