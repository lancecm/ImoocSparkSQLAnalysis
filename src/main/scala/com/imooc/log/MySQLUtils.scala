package com.imooc.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * MySQL工具类
  */
object MySQLUtils {
  /**
    * 获取数据库链接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=root")
  }

  /**
    * 释放数据库链接等资源
   */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try{
      if(pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]) = {
    println(getConnection())
  }
}
