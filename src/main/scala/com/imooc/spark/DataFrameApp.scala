package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * DataFrame API基本操作
 */
object DataFrameApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

    // 将json文件转换一个DataFrame
    var peopleDF = spark.read.format("json").load("file:///Users/Srunkyo/people.json")

    // 输出DataFrame对应的schema
    peopleDF.printSchema()

    // 输出数据集的前20条数据
    peopleDF.show()

    // 查询某一列所有的数据
    peopleDF.select("name").show()

    // 查询某几列所有的数据，进行列的计算,并起名 select name, age + 10 as age2 from table
    peopleDF.select(peopleDF.col("name"), peopleDF.col("age") + 10).as("age2").show()

    // 根据某一列的值进行过滤； select * from table where age > 19
    peopleDF.filter(peopleDF.col("age") > 19).show()

    // 根据某一列进行分组和聚合操作； select age, count(1) from table group by age
    peopleDF.groupBy("age").count().show()

    spark.close()
  }
}
