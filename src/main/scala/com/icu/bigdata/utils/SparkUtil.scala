package com.icu.bigdata.utils

import java.util.Properties

import com.icu.bigdata.model.Data.MySql1
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.ConnectionPool

/**
  *
  * sparkcontext
  * @author limeng
  * @create 2018-08-07 下午4:15
  **/
object SparkUtil {
  val logger: Logger = LoggerFactory.getLogger(SparkUtil.getClass)
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

  /**
    * 建立关系数据连接
    * @param driver
    * @param url
    */
  def relationDatabase(driver:String,url:String): Unit ={
    val urls=url.split("&")
    val user=urls.filter(line=>FilterUtils.isValidateUserName(line)).map(x=>{x.split(FilterUtils.userReg.regex).last}).mkString
    val password=urls.filter(line=>FilterUtils.isValidatePassword(line)).map(x=>{x.split(FilterUtils.passwordReg.regex).last}).mkString
    val host=urls.filter(line=>{!FilterUtils.isValidateUserName(line) && !FilterUtils.isValidatePassword(line)}).mkString("&")

    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }


  def relationDatabase(spark: SQLContext,mySql:MySql1): DataFrame ={
    val urls=mySql.url.split("&")
    val user=urls.filter(line=>FilterUtils.isValidateUserName(line)).map(x=>{x.split(FilterUtils.userReg.regex).last}).mkString
    val password=urls.filter(line=>FilterUtils.isValidatePassword(line)).map(x=>{x.split(FilterUtils.passwordReg.regex).last}).mkString
    val host=urls.filter(line=>{!FilterUtils.isValidateUserName(line) && !FilterUtils.isValidatePassword(line)}).mkString("&")

    val props = new Properties()
    props.put("user", user)
    props.put("password", password)

    spark.read.jdbc(host,mySql.tableName,props)
  }
}
