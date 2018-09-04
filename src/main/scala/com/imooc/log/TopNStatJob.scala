package com.imooc.log

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.functions._
/**
  *
  * 调优点2：
  * spark.sql.sources.partitionColumnTypeInference.enabled
  * 默认情况下参数设置为True，支持类型推测
  * 如果设置为False，则全部设置为String
  */
object TopNStatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopNStatJob").master("local[2]").
      config("spark.sql.sources.partitionColumnTypeInference.enabled", "false").getOrCreate()

    val accessDF = spark.read.format("parquet").load("/Users/Srunkyo/data/clean")

    accessDF.printSchema()
    accessDF.show(false)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF)


    spark.stop()
  }

  /**
    * 最受欢迎的TopN课程
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit = {

    /*
    第一种方式：使用DataFrame API进行统计
     */
    import spark.implicits._
    // 进行过滤, 按照天进行统计和排序
    var videoAccessTopNDF = accessDF.filter($"day" === "20170511" && $"cmsType" === "video").
      groupBy("day", "cmsId").agg(count("cmsId").
      as("times")).orderBy($"times".desc)

    videoAccessTopNDF.show(false)


    /*
    第二种方式：使用SparkSQL的方式进行统计
     */
    accessDF.createOrReplaceTempView("access_logs")
    var videoAccessTopNDFBySQL = spark.sql("select day, cmsId, count(1) as times from access_logs " +
      "where day='20170511' and cmsType='video' " +
      "group by day, cmsId order by times desc")
    videoAccessTopNDFBySQL.show(false)
  }

}
