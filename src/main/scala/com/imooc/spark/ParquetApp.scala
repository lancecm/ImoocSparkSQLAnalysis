package com.imooc.spark

import org.apache.spark.sql.SparkSession

/**
  * Spark 操作parquet/mysql等外部数据源
  * 如此一来scoop工具就没用了-》无需导数据
  */
object ParquetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()
    val path = "file:///home/hadoop/app/spark-2.1.0-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet"

    val userDF = spark.read.format("parquet").load(path)
    userDF.printSchema()
    userDF.show()
    userDF.select("name","favorite_color").show()

    // 写出标准写法。将parquet转换为json文件
    userDF.select("name","favorite_color").write.format("json").save("file:///home/hadoop/tmp/jsonout/")

    // 对于parquet文件，不指定文件类型也能读得出来（没指定format自动设置为parquet）
    // 还可以用spark-sql运行(见笔记或者官网)

    // 在生产环境中设置partition分区的数量 默认为200
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "200")

    // 操作Mysql
    // 方法1
    var jdbcDF = spark.read.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/sparksql?createDatabaseIfNotExist=true").
      option("dbtable","hive.TBLS").option("user","root").option("driver","com.mysql.jdbc.Driver").option("password", "root").load()

    // 方法2
    import java.util.Properties
    val connectionProperties = new Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password","root")
    connectionProperties.put("driver","com.mysql.jdbc.Driver")
    var jdbcDF2 = spark.read.jdbc("jdbc:mysql://localhost:3306", "hive.TBLS", connectionProperties)

    // 其他spark-sql的方式等请看官方文档


    spark.stop()
  }
}
