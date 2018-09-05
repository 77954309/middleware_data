package com.icu.bigdata.model

import com.icu.bigdata.utils.{SparkUtil, TimeUtils}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * ${DESCRIPTION}
  *
  * @author limeng
  * @create 2018-08-07 下午4:22
  **/
object Data {
  abstract class Data
  abstract class tables
  /**
    * hive
    * @param warehouseLocation 数据存储位置
    * @param dbName 数据库名称
    * @param tableName 表名称
    * @param year 年
    * @param month 月
    * @param day 日
    */
  case class Hive1(warehouseLocation:String,dbName:String,tableName:String,year:String,month:String,day:String)

  /**
    * hive
    * @param warehouseLocation 数据位置
    */
  case class Hive2(warehouseLocation:String,dbName:String,tableName:String) extends Data

  /**
    * hive
    * @param warehouseLocation 数据位置
    */
  case class Hive3(warehouseLocation:String,sparkSession:SparkSession) extends Data
  /**
    * mysql
    * @param url 路径带用户密码库名称
    * @param sql sql
    */
  case class MySql1(driver:String,url:String,tableName:String,sql:String) extends Data


  case class MySqlByTable(tableName:String,columns: String,values:Array[Row]) extends tables

  /**
    * mysql
    * @param url url
    * @param user 用户名称
    * @param password 密码
    * @param tableName 表名
    */
  case class MySql2(url:String,user:String,password:String,tableName:String) extends Data


   def callCase(f:Data): Any =f match {
    case Hive3(a,b)=>
      //SparkSession
      val appName:String="export_"+TimeUtils.getCertainDayTime(0)
      val sparkSession:SparkSession = SparkUtil.hiveSpark(appName,a)
      Hive3(a,sparkSession)

    case MySql1(a,b,c,d)=>SparkUtil.relationDatabase(a,b)
    case MySql2(a,b,c,d)=>SparkUtil.formMysql1(a,b,c)
  }
}
