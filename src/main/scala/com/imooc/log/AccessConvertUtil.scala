package com.imooc.log

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换（输入-》输出）工具类
  */
object AccessConvertUtil {
  // 定义输出的字段
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("city", StringType),
      StructField("time", StringType),
      StructField("day", StringType)
    )
  )

  /**
    * 根据输入的每一行信息转换成输出的样式
    * @param log 输入的每一行记录信息
    * 2017-05-11 08:07:35	http://www.imooc.com/article/17891	407	218.75.35.226
    */
  def parseLog(log: String): Row = { //注意返回类型的定义为Row
    try {
      val splits = log.split("\t") //进行分隔

      val url = splits(1)
      val traffic = splits(2).toLong //注意类型转换
      val ip = splits(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")

      var cmsType = "" // 注意此处设置为变量
      var cmsId = 0l
      if (cmsTypeId.length > 1) {
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      var city = IpUtils.getCity(ip)
      var time = splits(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      // Row中的字段要和Struct中的字段对应上
      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    }
    catch {
      case e: Exception => Row(0)
    }
  }
}
