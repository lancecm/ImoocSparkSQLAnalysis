package com.imooc.spark

import java.sql.DriverManager


/**
  * 通过jdbc的方式编程
  * 注意：在使用jdbc开发时要启动thrift server
  *
    thriftserver/beeline的使用
    1) 启动thriftserver: 默认端口是10000 ，可以修改
    2）启动beeline
    beeline -u jdbc:hive2://localhost:10000 -n hadoop


    修改thriftserver启动占用的默认端口号：
    ./start-thriftserver.sh  \
    --master local[2] \
    --jars ~/software/mysql-connector-java-5.1.27-bin.jar  \
    --hiveconf hive.server2.thrift.port=14000

    beeline -u jdbc:hive2://localhost:14000 -n hadoop

    thriftserver和普通的spark-shell/spark-sql有什么区别？
  1）spark-shell、spark-sql都是一个spark  application；
  2）thriftserver， 不管你启动多少个客户端(beeline/code)，永远都是一个spark application
    解决了一个数据共享的问题，多个客户端可以共享数据；
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
     Class.forName("org.apache.hive.jdbc.HiveDriver")
    var conn = DriverManager.getConnection("jdbc:hive2://hadoop001:10000","hadoop","");
    var pstmt = conn.prepareStatement("select empno, ename, sal from emp")
    var rs = pstmt.executeQuery()
    while (rs.next()) {
      print("empno: " + rs.getInt("empno") +
        ", ename:" + rs.getString("ename") +
        ", sal: " + rs.getDouble("sal")
      )
    }

    // 生产需要写try catch
    rs.close()
    pstmt.close()
    conn.close()
  }
}
