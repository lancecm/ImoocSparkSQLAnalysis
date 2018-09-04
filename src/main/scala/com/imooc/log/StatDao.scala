package com.imooc.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * 各个维度统计的DAO操作
  */
object StatDao {
  /**
    * 批量保存DayVideoAccessTopN对象到数据库
    * @param list
    */
  def insertDayVideoAccessTopN(list: ListBuffer[DayVideoAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 设置手动提交

      val sql = "insert into day_video_access_topn_stat(day, cms_id, times) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.times)

        pstmt.addBatch()
      }

      // 调优点：执行批次处理
      pstmt.executeBatch()
      connection.commit() // 手动提交

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }


  /**
    * 批量存储DayCityVideoAccessStat到数据库
    */
  def insertDayCityVideoAccessTopN(list: ListBuffer[DayVideoCityAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 设置手动提交

      val sql = "insert into day_video_city_access_topn_stat(day, cms_id, city, times, times_rank) values(?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setString(3, ele.city)
        pstmt.setLong(4, ele.times)
        pstmt.setInt(5, ele.timesRank)

        pstmt.addBatch()
      }

      // 调优点：执行批次处理
      pstmt.executeBatch()
      connection.commit() // 手动提交

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  /**
    * 批量存储DayVideoAccessStat到数据库
    */
  def insertDayTrafficsVideoAccessTopN(list: ListBuffer[DayVideoTrafficsAccessStat]) = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false) // 设置手动提交

      val sql = "insert into day_video_traffic_access_topn_stat(day, cms_id, traffics) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      for (ele <- list) {
        pstmt.setString(1, ele.day)
        pstmt.setLong(2, ele.cmsId)
        pstmt.setLong(3, ele.traffics)
        pstmt.addBatch()
      }

      // 调优点：执行批次处理
      pstmt.executeBatch()
      connection.commit() // 手动提交

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  /**
    * 删除指定日期的数据
    * @param day
    */
  def deleteData(day: String): Unit = {
    val tables = Array("day_video_access_topn_stat",
      "day_video_city_access_topn_stat",
      "day_video_traffic_access_topn_stat")
    var connection:Connection = null
    var pstmt: PreparedStatement = null
    try {
      connection = MySQLUtils.getConnection()
      for (table <- tables) {
        // delete from table ...
        val deleteSQL = s"delete from $table where day = ?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1, day)
        pstmt.execute()
      }
    } catch {
       case e: Exception => e.printStackTrace()
    } finally {
       MySQLUtils.release(connection, pstmt)
    }
  }


}
