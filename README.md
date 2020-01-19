# PkpmSpark
大数据分析 三维数据挖掘运算分析程序
## 机理
获取mongodb中存储的三维数据进行分析,归并,聚合,加权运算，最后计算完成后的结果存入redis
## 环境配置
1.mongodb中需含有名称为mydb数据库与名为netflows的collection

2.需要配置kafka 并新建topic名称为order(若不需要由kafka做触发源则不用配置)

3.需配置redis，无用户名密码登录

4.需spark运行环境 2.4.4测试通过

5.需scala sdk 2.11.12测试通过
## 程序配置
1.kafka消费实例配置在OrderTopicKafka类中，请跟据需要修改，可由工厂模式附加新构造上去

2.redis链接实例配置在RedisConnector类中，请跟据需要修改

3.mongodb链接实例配置在SparkMainTask的main函数中，请跟据需要修改
## 编译
本项目配置了maven编译，在idea命令行执行`mvn assembly:assembly`即可编译生成
## 数据源
本项目数据源来自mongodb，数据采集可以传入kafka借由如下链接项目完成kafka向mongodb的同步，采集端只需保证json格式：

[https://github.com/MethodJiao/Kafka2Mongodb](https://github.com/MethodJiao/Kafka2Mongodb)


也可直接在mongodb中仿造执行如下insert语句制造数据，数据格式如下：

1.ChildNode中数组可嵌入多个立方体以此描述三维空间

2.HighPt，LowPt分别为立方体体的对角线端点两点

3.Name可以存储当前立方体表述对象名

4.Origin为当前立方体的原点定位

5.YPRangle为立方体姿态角
```
db.getCollection("netflows").insert( {
    _id: ObjectId("5dededecb3e6784f020c4e90"),
    RootNode: {
        ChildNode: [
            {
                HighPt: {
                    x: 83956,
                    y: 76703,
                    z: 2900
                },
                IsSet: {
                    set: false
                },
                LowPt: {
                    x: 80155,
                    y: 76502,
                    z: 2500
                },
                Name: {
                    name: "PBStructBeam"
                },
                Origin: {
                    x: 83955.9781857537,
                    y: 76602.633138063,
                    z: 2900
                },
                YPRangle: {
                    pitch: 0,
                    roll: 90,
                    yaw: -90
                }
            },
            {
                HighPt: {
                    x: 83557,
                    y: 74903,
                    z: 2900
                },
                IsSet: {
                    set: false
                },
                LowPt: {
                    x: 80756,
                    y: 74702,
                    z: 2420
                },
                Name: {
                    name: "PBStructBeam"
                },
                Origin: {
                    x: 83556.179,
                    y: 74802.2127855456,
                    z: 2900
                },
                YPRangle: {
                    pitch: 0,
                    roll: 90,
                    yaw: -90
                }
            }
        ],
        KeyValue: NumberInt("-6567")
    }
} );
```
## 运行
示例：
```
bin/spark-submit --class spark.bim.SparkMainTask --master spark://10.100.140.35:7077 /Users/method.jiao/code/pkpmspark/target/pkpmspark-1.0-SNAPSHOT-jar-with-dependencies.jar
```
参数修改：

1.配置ip 10.100.140.35:7077

2.jar包编译路径 /Users/method.jiao/code/pkpmspark/target/pkpmspark-1.0-SNAPSHOT-jar-with-dependencies.jar

# PS
1.如果想在idea直接本地启动不上传task到spark集群的话需要在sparksession加个.master("local")如：
```
val sparkSession = SparkSession.builder()
  .appName("PKPMBimAnalyse")
  .config("spark.mongodb.input.uri", "mongodb://10.100.140.35/mydb.netflows")
  .master("local")
  .getOrCreate()
```
2.如果上传task到集群务必去掉master属性

3.如果想在idea远程提交并调试 如：
```
val sparkSession = SparkSession.builder()
  .appName("PKPMBimAnalyse")
  .config("spark.mongodb.input.uri", "mongodb://10.100.140.35/mydb.netflows")
  .master("spark://10.100.140.35:7077")
  .getOrCreate()
```
