package utils

import org.jfree.chart.{ChartFactory, ChartPanel, JFreeChart}
import org.jfree.data.time.{Second, TimeSeriesCollection, TimeSeries}
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.chart.axis.{DateAxis, NumberAxis}
import java.awt.{Color, BasicStroke, Dimension, Font, BorderLayout, GridLayout}
import javax.swing.{JFrame, WindowConstants, JPanel, JComboBox, JCheckBox, JLabel, JScrollPane, BoxLayout}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import javax.swing.border.EmptyBorder

object StationChartGenerator {
  private var frame: JFrame = _
  private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
  
  // 懒加载数据集
  private lazy val stationDataset = new TimeSeriesCollection()
  
  // 懒加载图表
  private lazy val stationChart = createStationChart()
  
  // 数据系列映射
  private val stationSeriesMap = scala.collection.mutable.Map[String, Map[String, TimeSeries]]()
  
  // 当前选择的线路和站点
  private var selectedLine = "A"
  private val selectedStations = scala.collection.mutable.Set[String]()
  private val selectedTypes = scala.collection.mutable.Set("total") // 默认显示总流量
  
  // 流量类型映射
  private val flowTypes = Map(
    "in" -> "入站",
    "out" -> "出站",
    "total" -> "总流量"
  )

  // 站点基础颜色映射
  private val stationBaseColors = scala.collection.mutable.Map[String, Color]()
  
  // 流量类型的颜色调整因子
  private val typeColorAdjust = Map(
    "total" -> (1.0f, 1.0f, 1.0f),    // 原色
    "in" -> (0.8f, 1.2f, 0.8f),       // 偏绿
    "out" -> (1.2f, 0.8f, 0.8f)       // 偏红
  )

  def initChart(): Unit = {
    // 创建主面板
    val mainPanel = new JPanel(new BorderLayout())
    
    // 创建控制面板
    val controlPanel = new JPanel()
    controlPanel.setLayout(new BoxLayout(controlPanel, BoxLayout.Y_AXIS))
    controlPanel.setBorder(new EmptyBorder(5, 5, 5, 5))

    // 线路选择面板
    val linePanel = new JPanel()
    linePanel.add(new JLabel("线路选择: "))
    val lineSelector = new JComboBox[String](Array("A", "B", "C"))
    lineSelector.addActionListener(_ => {
      selectedLine = lineSelector.getSelectedItem.toString
      updateStationCheckboxes()
      updateSeriesVisibility()
    })
    linePanel.add(lineSelector)
    controlPanel.add(linePanel)

    // 流量类型选择面板
    val typePanel = new JPanel()
    typePanel.add(new JLabel("流量类型: "))
    flowTypes.foreach { case (typeId, typeName) =>
      val checkbox = new JCheckBox(typeName)
      checkbox.setSelected(typeId == "total")
      checkbox.addActionListener(_ => {
        if (checkbox.isSelected) {
          selectedTypes.add(typeId)
        } else {
          selectedTypes.remove(typeId)
        }
        updateSeriesVisibility()
      })
      typePanel.add(checkbox)
    }
    controlPanel.add(typePanel)

    // 站点选择面板（使用滚动面板）
    val stationPanel = new JPanel()
    stationPanel.setLayout(new BoxLayout(stationPanel, BoxLayout.Y_AXIS))
    val scrollPane = new JScrollPane(stationPanel)
    scrollPane.setPreferredSize(new Dimension(200, 300))
    controlPanel.add(scrollPane)
    
    // 保存站点选择面板的引用，以便后续更新
    controlPanel.putClientProperty("stationPanel", stationPanel)

    // 创建图表面板
    val chartPanel = createChartPanel(stationChart)
    
    // 添加到主面板
    mainPanel.add(controlPanel, BorderLayout.WEST)
    mainPanel.add(chartPanel, BorderLayout.CENTER)

    // 创建并配置主窗口
    frame = new JFrame("Station Passenger Flow Analysis")
    frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
    frame.setContentPane(mainPanel)
    frame.setSize(1200, 700)
    frame.setLocationRelativeTo(null)
    
    javax.swing.SwingUtilities.invokeLater(() => frame.setVisible(true))
  }

  private def updateStationCheckboxes(): Unit = {
    val controlPanel = frame.getContentPane.getComponent(0).asInstanceOf[JPanel]
    val stationPanel = controlPanel.getClientProperty("stationPanel").asInstanceOf[JPanel]
    
    // 清除现有的复选框
    stationPanel.removeAll()
    selectedStations.clear()
    
    // 添加新的复选框
    stationSeriesMap.keys
      .filter(_.startsWith(s"$selectedLine-"))
      .toSeq.sorted
      .foreach { stationKey =>
        val checkbox = new JCheckBox(s"站点 ${stationKey.split("-")(1)}")
        checkbox.addActionListener(_ => {
          if (checkbox.isSelected) {
            selectedStations.add(stationKey)
          } else {
            selectedStations.remove(stationKey)
          }
          updateSeriesVisibility()
        })
        stationPanel.add(checkbox)
      }
    
    // 刷新面板
    stationPanel.revalidate()
    stationPanel.repaint()
  }

  private def createChartPanel(chart: JFreeChart): JPanel = {
    val panel = new ChartPanel(chart)
    panel.setPreferredSize(new Dimension(800, 600))
    panel.setMouseWheelEnabled(true)
    panel.setDomainZoomable(true)
    panel.setRangeZoomable(true)
    panel
  }

  private def createStationChart(): JFreeChart = {
    val chart = ChartFactory.createTimeSeriesChart(
      "站点客流量分析",
      "时间",
      "人数",
      stationDataset,
      true,
      true,
      false
    )
    
    configureChart(chart)
    chart
  }

  private def configureChart(chart: JFreeChart): Unit = {
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    val renderer = new XYLineAndShapeRenderer(true, false)
    
    // 配置时间轴
    val dateAxis = plot.getDomainAxis.asInstanceOf[DateAxis]
    dateAxis.setDateFormatOverride(new SimpleDateFormat("HH:mm"))
    dateAxis.setLabelFont(new Font("微软雅黑", Font.BOLD, 14))
    dateAxis.setTickLabelFont(new Font("微软雅黑", Font.PLAIN, 12))
    
    // 配置数值轴
    val numberAxis = plot.getRangeAxis.asInstanceOf[NumberAxis]
    numberAxis.setLabelFont(new Font("微软雅黑", Font.BOLD, 14))
    numberAxis.setTickLabelFont(new Font("微软雅黑", Font.PLAIN, 12))
    numberAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits())
    
    // 设置图表外观
    plot.setRenderer(renderer)
    plot.setBackgroundPaint(Color.WHITE)
    plot.setDomainGridlinePaint(new Color(220, 220, 220))
    plot.setRangeGridlinePaint(new Color(220, 220, 220))
    
    // 设置图例
    chart.getLegend.setItemFont(new Font("微软雅黑", Font.BOLD, 12))
    chart.getTitle.setFont(new Font("微软雅黑", Font.BOLD, 16))
  }

    // 生成分散的HSB颜色
  private def generateStationColor(stationKey: String): Color = {
    val lineId = stationKey.split("-")(0)
    val stationId = stationKey.split("-")(1).toInt
    
    // 根据线路设置基础色相范围
    val baseHue = lineId match {
      case "A" => 0.0f      // 红色范围
      case "B" => 0.3f      // 绿色范围
      case "C" => 0.6f      // 蓝色范围
      case _ => 0.0f
    }
    
    // 在该范围内根据站点ID生成分散的色相
    val hueOffset = ((stationId * 0.618033988749895) % 0.2).toFloat  // 黄金分割比
    val hue = (baseHue + hueOffset) % 1.0f
    
    Color.getHSBColor(hue, 0.8f, 0.9f)
  }

  // 根据基础颜色和流量类型生成最终颜色
  private def adjustColorForType(baseColor: Color, typeId: String): Color = {
    val (redFactor, greenFactor, blueFactor) = typeColorAdjust(typeId)
    
    new Color(
      Math.min(255, Math.max(0, (baseColor.getRed * redFactor).toInt)),
      Math.min(255, Math.max(0, (baseColor.getGreen * greenFactor).toInt)),
      Math.min(255, Math.max(0, (baseColor.getBlue * blueFactor).toInt))
    )
  }

  private def updateSeriesVisibility(): Unit = {
    val plot = stationChart.getPlot.asInstanceOf[XYPlot]
    val renderer = plot.getRenderer
    
    stationSeriesMap.foreach { case (stationKey, typeSeries) =>
      typeSeries.foreach { case (typeId, series) =>
        val index = stationDataset.indexOf(series)
        if (index >= 0) {
          val isVisible = selectedStations.contains(stationKey) && selectedTypes.contains(typeId)
          renderer.setSeriesVisible(index, isVisible)
        }
      }
    }
  }

  private def createNewSeries(name: String, stationKey: String, typeId: String): TimeSeries = {
    val series = new TimeSeries(name)
    stationDataset.addSeries(series)
    
    // 获取或生成站点基础颜色
    val baseColor = stationBaseColors.getOrElseUpdate(stationKey, generateStationColor(stationKey))
    
    // 根据流量类型调整颜色
    val finalColor = adjustColorForType(baseColor, typeId)
    
    // 设置系列的样式
    val plot = stationChart.getPlot.asInstanceOf[XYPlot]
    val renderer = plot.getRenderer
    val index = stationDataset.getSeriesCount - 1
    
    renderer.setSeriesPaint(index, finalColor)
    renderer.setSeriesStroke(index, new BasicStroke(2.0f))
    renderer.setSeriesVisible(index, false)  // 默认不显示
    
    series
  }

  def updateChart(stationData: Map[String, (Int, Int, Int)], timeStr: String): Unit = {
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
          
          // 处理每个站点的数据
          stationData.foreach { case (stationKey, (inCount, outCount, totalCount)) =>
            // 获取或创建该站点的所有类型的时间序列
            val typeSeries = stationSeriesMap.getOrElseUpdate(stationKey, {
              val newSeries = Map(
                "in" -> createNewSeries(s"$stationKey (入站)", stationKey, "in"),
                "out" -> createNewSeries(s"$stationKey (出站)", stationKey, "out"),
                "total" -> createNewSeries(s"$stationKey (总流量)", stationKey, "total")
              )
              updateStationCheckboxes()  // 添加新站点后更新复选框
              newSeries
            })
            
            // 更新数据点
            typeSeries("in").addOrUpdate(second, inCount)
            typeSeries("out").addOrUpdate(second, outCount)
            typeSeries("total").addOrUpdate(second, totalCount)
          }
          
          // 打印调试信息
          println(s"\nUpdated station chart at $timeStr")
          stationData.filter(_._1.startsWith(selectedLine)).foreach { case (stationKey, (in, out, total)) =>
            println(f"Station $stationKey: in=$in%d, out=$out%d, total=$total%d")
          }
        } catch {
          case e: Exception =>
            println(s"Error updating station chart: ${e.getMessage}")
            e.printStackTrace()
        }
      })
    } catch {
      case e: Exception =>
        println(s"Error in station chart update: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}