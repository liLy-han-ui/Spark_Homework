# Spark_Homework
## 1. 实时数据处理
文件来源：阿里天池数据集 https://tianchi.aliyun.com/dataset/21904
### （1）subway-analysis：使用kafka模拟实时数据流，structured streaming进行处理，存储到hbase中
#### 准备工作：
##### 创建kafka主题：
kafka-topics --create --topic subway_data --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
##### 创建数据表：
create 'line_statistics','cf'
create 'station_statistics','cf'
#### 编译项目：
sbt clean compile
#### 先运行 Consumer（新开一个终端）：
sbt "runMain consumer.MetroDataConsumer"
#### 再运行 Producer（新开另一个终端）：
sbt "runMain producer.MetroDataProducer"
### （2）metro-analysis：使用kafka消费者和生产者进行处理
#### 准备工作：
##### 创建kafka主题：
kafka-topics --create --topic metro-data --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
##### 创建数据表：
create 'metro_line_stats', 'stats'
create 'metro_station_stats', 'stats'
#### 编译项目：
sbt clean compile
#### 先运行 Consumer（新开一个终端）：
sbt "runMain consumer.MetroDataConsumer"
#### 再运行 Producer（新开另一个终端）：
sbt "runMain producer.MetroDataProducer"
## 2. 离线数据处理
ApplyOne：数据预处理
ApplyTwo：一周的每天总客流量对比
ApplyTree：一周的每天线路客流量进行对比处理
ApplyFour：每日的站点客流统计
ApplyFive：每日总客流的柱状图统计
ApplySix：支付方式统计
