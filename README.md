# PkpmSpark
大数据处理框架spark运算程序
## 机理
由kafka触发本程序运算，计算完成后的结果存入redis
## 环境配置
1.mongodb中需含有名称为mydb数据库与名为netflows的collection

2.本机需要配置kafka 并新建topic名称为order

3.本机需配置redis，无用户名密码登录
## 程序配置
1.kafka消费实例配置在OrderTopicKafka类中，请跟据需要修改，可由工厂模式附加新构造上去

2.redis链接实例配置在RedisConnector类中，请跟据需要修改

3.mongodb链接实例配置在SparkMainTask的main函数中，请跟据需要修改
## 编译
本项目配置了maven编译，在idea命令行执行`mvn assembly:assembly`即可编译生成
##运行
示例：
```
bin/spark-submit --class spark.bim.SparkMainTask --master spark://10.100.140.35:7077 /Users/method.jiao/code/pkpmspark/target/pkpmspark-1.0-SNAPSHOT-jar-with-dependencies.jar
```
参数修改：

1.配置ip 10.100.140.35:7077

2.jar包编译路径 /Users/method.jiao/code/pkpmspark/target/pkpmspark-1.0-SNAPSHOT-jar-with-dependencies.jar

## PS
如果想在idea直接启动不上传task到spark集群的话需要在sparksession加个.master("local")如：
```
val sparkSession = SparkSession.builder()
  .appName("PKPMBimAnalyse")
  .config("spark.mongodb.input.uri", "mongodb://10.100.140.35/mydb.netflows")
  .master("local")
  .getOrCreate()
```
如果上传task到集群务必去掉master属性