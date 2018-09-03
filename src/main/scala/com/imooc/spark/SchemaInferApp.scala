package com.imooc.spark

import org.apache.spark.sql.SparkSession

object SchemaInferApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()
    val path = "file:///Users/Srunkyo/data/json_schema_infer.json"
    val DF = spark.read.format("json").load(path)
    DF.printSchema()
    DF.show()
    spark.close()
  }
}
