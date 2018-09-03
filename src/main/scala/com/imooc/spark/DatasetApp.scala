package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * Datasets的使用
  * Dataset：强类型 typed case class
  * DataFrame：弱类型 Row
  *
  * 静态安全性比较。
  * SQL：
  *   seletc name from person; 编译通过 运行报错
  * DataFrame：
  *   df.seletc("namme") 编译不通过
  *   df.select("name1") 编译通过
  * Dataset：
  *   df.select("name1") 编译不通过  更早发现错误
  */
object DatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()
    val path = "file:///Users/Srunkyo/data/sales.csv"
    // 转换csv文件
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show()

    // 注意：需要导入隐式转换
    import spark.implicits._
    val ds = df.as[Sales]
    ds.map(line => line.itemId).show()

    spark.stop()
  }

  case class Sales(transactionId: Int,customerId: Int,itemId: Int,amountPaid: Double)
}
