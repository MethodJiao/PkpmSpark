# PkpmSpark
大数据处理框架spark运算程序
## 机理
由kafka触发本程序运算，计算完成后的结果存入redis
## 配置
1.mongodb中需含有名称为mydb数据库与名为netflows的collection

2.本机需要配置kafka 并新建topic名称为flume1

3.本机需配置redis，无用户名密码登录