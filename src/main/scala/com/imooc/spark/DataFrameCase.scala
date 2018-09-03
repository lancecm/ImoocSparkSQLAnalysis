package com.imooc.spark

import com.imooc.spark.DataFrameRDDApp.Info
import org.apache.spark.sql.SparkSession

/**
  * DataFrame中的操作
  *
  * 可以在IDEA中写完代码，贴到控制台spark环境中用spark-shell运行。
  */
object DataFrameCase {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()
    val rdd = spark.sparkContext.textFile("file:///Users/Srunkyo/data/student.data")

    // 注意转义字符的使用。
    import spark.implicits._
    val infoDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    // 默认只显示前20条，超过一定长度的数据会省略显示
    infoDF.show()
    // 取30条，不截取显示
    infoDF.show(30, false)
    infoDF.take(10)
    infoDF.take(10).foreach(println)
    infoDF.first()
    infoDF.head(3)
    infoDF.select("email").show(30, false)
    // 过滤
    infoDF.filter("name = '' OR name = 'NULL'").show()
    infoDF.filter("SUBSTR(name, 0, 1)='M'").show()
    // 排序
    infoDF.sort(infoDF("name")).show()
    infoDF.sort(infoDF("name").desc).show()
    infoDF.sort(infoDF("name").asc, infoDF("id").desc).show()
    // 重命名
    infoDF.select(infoDF("name").as("Student Name")).show()

    // join  注意用三个等号
    val infoDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()
    infoDF.join(infoDF2, infoDF.col("id") === infoDF2.col("id")).show()

    spark.close()
  }

  case class Student(id:Int, name:String, phone:String, email:String)
}
