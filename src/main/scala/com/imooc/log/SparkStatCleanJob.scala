package com.imooc.log

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 使用Spark完成数据清洗操作
  * 1. RDD->DF
  * 2. IP地址->城市信息
  *
  * 1）git clone https://github.com/wzhe06/ipdatabase.git
  * 2）编译下载的项目：mvn clean package -DskipTests
  * 3）安装jar包到自己的maven仓库
  *   mvn install:install-file -Dfile=/Users/Srunkyo/ipdatabase/target/ipdatabase-1.0-SNAPSHOT.jar -DgroupId=com.ggstar -DartifactId=ipdatabase -Dversion=1.0 -Dpackaging=jar
  */
object SparkStatCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatCleanJob").master("local[2]").getOrCreate()


    val accessRDD = spark.sparkContext.textFile("/Users/Srunkyo/data/access.log")
    // accessRDD.take(10).foreach(println)

    // RDD->DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)), AccessConvertUtil.struct)
//    accessDF.printSchema()
//    accessDF.show(false)

    // DF放入存储系统, 按照天进行分区
    // mode(SaveMode.Overwrite) 调整成覆盖模式
    // 调优点：控制文件输出的大小 coalesce(1) 使得输出文件更大
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
      .partitionBy("day")
        .save("/Users/Srunkyo/data/clean")
    
    spark.stop()
  }
}
