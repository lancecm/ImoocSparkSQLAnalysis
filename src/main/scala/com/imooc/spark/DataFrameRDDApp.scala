package com.imooc.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
/**
  * DataFrame和RDD的互操作的两种方式
  * 1. 反射：case class 前提：实现需要知道你的字段和字段类型
  * 2. 编程：Row        如果第一种情况不能满足要求，即实现不能知道列
  * 3. 选型：优先考虑第一种
  */
object DataFrameRDDApp {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

//    inferReflection(spark)

    program(spark)

    spark.stop()
  }

  // 通过编程地方式进行RDD -> DataFrame 转换
  def program(spark: SparkSession): Unit = {
    // RDD -> DataFrame （使用反射的方式来腿短包含了特定数据类型的RDD元数据， 先定义一个case class）
    // 面向dataframe进行编程
    val rdd = spark.sparkContext.textFile("file:///Users/Srunkyo/infos.txt")

    // 三个步骤：
    /*
      1. 使用ROW将数据转换为RDD
      2. 构建StructType
      3. 进行转换
     */
    val infoRDD = rdd.map(_.split(",")).map(line => Row(line(0).toInt, line(1), line(2).toInt))
    var structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    val infoDf = spark.createDataFrame(infoRDD, structType)
    infoDf.printSchema()
    infoDf.show()
  }


  // RDD -> DataFrame （使用反射的方式来腿短包含了特定数据类型的RDD元数据， 先定义一个case class）
  private def inferReflection(spark: SparkSession) = {
    // 面向dataframe进行编程
    val rdd = spark.sparkContext.textFile("file:///Users/Srunkyo/infos.txt")

    // 注意：需要导入隐式转换
    import spark.implicits._
    val infoDF = rdd.map(_.split(",")).map(line => Info(line(0).toInt, line(1), line(2).toInt)).toDF()

    infoDF.show()

    // 通过df的api进行操作
    infoDF.filter(infoDF.col("age") > 30).show()

    // 通过sql的方式进行操作
    infoDF.createOrReplaceTempView("infos")
    spark.sql("select * from infos where age > 30")
  }

  case class Info(id: Int, name: String, age: Int)
}
