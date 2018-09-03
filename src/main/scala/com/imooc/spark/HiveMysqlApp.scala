package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * 使用外部数据源综合查询Hive和Mysql的表数据
  */
object HiveMysqlApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveMysqlApp").master("local[2]").getOrCreate()

    // 加载hive数据
    val hiveDF = spark.table("emp")
    // 加载MYSQL表
    val mysqlDF = spark.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306").
      option("dbtable","spark.DEPT").option("user","root").
      option("driver","com.mysql.jdbc.Driver").
      option("password", "root").load()

    // Join操作
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.show()

    resultDF.select(hiveDF.col("empno"), hiveDF.col("ename"), mysqlDF.col("deptno"), mysqlDF.col("dname")).show()

    spark.stop()

  }
}
