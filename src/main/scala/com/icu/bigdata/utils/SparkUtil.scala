package com.icu.bigdata.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scalikejdbc.ConnectionPool

/**
  *
  * sparkcontext
  * @author limeng
  * @create 2018-08-07 下午4:15
  **/
object SparkUtil {
  /**
    * hive
    * @param warehouseLocation 数据位置
    * @return
    */
  def hiveSpark(appName:String,warehouseLocation:String): SparkSession ={
    val conf=new SparkConf().setAppName(appName).setMaster("local[4]")
    val hiveSpark=SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    hiveSpark
  }

  /**
    * 从库环境
    */
  def formMysql1(host:String,user:String,password:String): Unit ={
    val driver="com.mysql.jdbc.Driver"

    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }
}
