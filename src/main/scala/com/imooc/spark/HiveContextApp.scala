package com.imooc.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * HiveContext的使用
  * 使用时需要通过--jars 把mysql驱动传进来
  *
  * 执行脚本之前需要赋予执行权限
  * chmod -ux xx
  *
  * spark-submit \
  * --class com.imooc.spark.HiveContextApp \
  * --master local[2] \
  * --jars /home/hadoop/software/mysql-connector-java-5.1.27-bin.jar \
  * /home/hadoop/lib/sql-1.0.jar
  *
  * 常常会出现com.mysql.jdbc.driver not found错误，需要在脚本上指定驱动包或者将驱动配置环境变量。
  *
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    //1. 创建相应的Context
    val sparkConf = new SparkConf()
    // 在测试或者生产中，AppName和Master我们是通过脚本进行指定的
    //    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2. 相关的处理:
    hiveContext.table("emp").show

    //3. 关闭资源
    sc.stop()
  }
}
