package com.icu.bigdata.utils

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * sql
  *
  * @author limeng
  * @create 2018-08-07 下午6:42
  **/
object SqlUtil {
  /**
    * 禁止严格模式 查询
    * @param sqlContext
    * @param sqlModel sql
    */
    def queryByHive(sqlContext:SQLContext,sqlModel:SqlModel2): DataFrame ={
      sqlContext.sql(s"USE ${sqlModel.db}")
      sqlContext.sql("SET hive.mapred.mode=nonstrict")
      sqlContext.sql(s"select * from ${sqlModel.tableName}").drop("year","month","day")
    }

  /**
    * 查询分区表
    * @param sqlContext
    * @param sqlModel sql
    */
  def queryByHiveToPartition(sqlContext:SQLContext,sqlModel:SqlModel1): DataFrame ={
    sqlContext.sql(s"USE ${sqlModel.db}")
    sqlContext.sql("SET hive.exec.dynamic.partition=true")
    sqlContext.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    sqlContext.sql(s"select * from ${sqlModel.tableName} where year=${sqlModel.year} and month=${sqlModel.month} and day=${sqlModel.day}").drop("year","month","day")
  }
}
case class SqlModel1(db:String,tableName:String,year:String,month:String,day:String)
case class SqlModel2(db:String,tableName:String)