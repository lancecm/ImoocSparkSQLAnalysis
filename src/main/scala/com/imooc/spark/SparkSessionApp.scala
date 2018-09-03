package com.imooc.spark

import org.apache.spark.sql.SparkSession


object SparkSessionApp {
  /**
    * SparkSession的使用 (spark2.0的用法)
    */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
//    spark.read.format("json")
    val people = spark.read.json("file:///Users/Srunkyo/people.json")
    people.show()

    spark.stop()
  }
}
