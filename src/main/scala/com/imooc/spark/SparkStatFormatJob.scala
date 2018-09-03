package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们所需要对指定的列的数据
  * 抽取日志的一部分：
  * head -10000 access.20161111.log >> 10000_access.log
  *
  * 117.35.88.11 - - [10/Nov/2016:00:01:02 +0800] "GET /article/ajaxcourserecommends?id=124 HTTP/1.1" 200 2345 "www.imooc.com" "http://www.imooc.com/code/1852" - "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36" "-" 10.100.136.65:80 200 0.616 0.616
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val access = spark.sparkContext.textFile("file:///Users/Srunkyo/data/10000_access.log")
    access.take(10).foreach(println)
    access.map(line => {
      val splits = line.split(" ")
      val ip = splits(0)
      val time = splits(3) + " " + splits(4) // 原始日志的第三个和第四个字段拼接起来就是完整的访问时间 yyyy-MM-dd HH:mm:ss
      val url = splits(11)
      val traffic = splits(9)
      (ip, DateUtils.parse(time), url, traffic)

      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("file:///Users/Srunkyo/data/output2")
    spark.stop()
  }
}
