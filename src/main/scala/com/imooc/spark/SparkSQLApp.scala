package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/*
  SQLContext的使用
  注意：IDEA在本地，而数据在服务器上面

  编译：
  进入目录
  mvn clean package -DskipTests
  文件上传
  scp sql-1.0.jar hadoop@192.168.0.103:~/lib/
  提交Spark Application 到环境中运行
   spark-submit \
   --name SQLContextApp
   --class com.imooc.spark.SparkSQLApp \
   --master local[2] \
  /home/hadoop/lib/sql-1.0.jar \
  /home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json
 */
object SparkSQLApp {

  def main(args: Array[String]): Unit = {
    val path = args(0)
    //1. 创建相应的Context
    val sparkConf = new SparkConf()
    // 在测试或者生产中，AppName和Master我们是通过脚本进行指定的
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")


    val sc = new SparkContext(sparkConf)
    val sqlContest = new SQLContext(sc)

    //2. 相关的处理: 处理json文件
    val people = sqlContest.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3. 关闭资源
    sc.stop()
  }
}
