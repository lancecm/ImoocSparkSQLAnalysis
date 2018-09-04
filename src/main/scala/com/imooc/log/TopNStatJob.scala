package com.imooc.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
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

//    accessDF.printSchema()
//    accessDF.show(false)

    val day = "20170511"
    //每次执行前需要把当前数据库删除
    StatDao.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    //按照城市统计最受欢迎的TopN课程
    cityAccessTopNStat(spark, accessDF, day)

    //按照流量进行统计
    videoTrafficsTopNStat(spark, accessDF, day)

    spark.stop()
  }

  /**
    * 按照流量进行统计
    * @param spark
    * @param accessDF
    */
  def videoTrafficsTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._
    // 进行过滤, 按照天进行统计和排序
    var trafficsAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video").
      groupBy("day", "cmsId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    trafficsAccessTopNDF.show(false)

    // 写入MySQL
    try{
      trafficsAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoTrafficsAccessStat]

        // 每条数据拆分并构建对象
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsAccessStat(day, cmsId, traffics))
        })

        // 批次插入数据
        StatDao.insertDayTrafficsVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照地市统计TopN课程
    */
  def cityAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String) = {
    import spark.implicits._
    // 进行过滤, 按照天进行统计和排序
    var cityAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video").
      groupBy("day","city", "cmsId").
      agg(count("cmsId").as("times")).
      orderBy($"times".desc)
    cityAccessTopNDF.show(false)

    // Window函数在SparkSQL中的使用
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city")).orderBy(cityAccessTopNDF("times").desc)).as("times_rank")
    ).filter("times_rank <= 3") // 各地区Top3最受欢迎的课程
    top3DF.show(false)

    //数据写入MySQL中
    /*
    将统计结果写入到MySQL中
     */
    try{
      // DF切分
      top3DF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoCityAccessStat]

        // 每条数据拆分并构建对象
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val city = info.getAs[String]("city")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")

          list.append(DayVideoCityAccessStat(day, cmsId, city, times, timesRank))
        })

        // 批次插入数据
        StatDao.insertDayCityVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 最受欢迎的TopN课程
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {

    /*
    第一种方式：使用DataFrame API进行统计
     */
    import spark.implicits._
    // 进行过滤, 按照天进行统计和排序
    var videoAccessTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video").
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

    /*
    将统计结果写入到MySQL中
     */
    try{
      videoAccessTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[DayVideoAccessStat]

        // 每条数据拆分并构建对象
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          list.append(DayVideoAccessStat(day, cmsId, times))
        })

        // 批次插入数据
        StatDao.insertDayVideoAccessTopN(list)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }


}
