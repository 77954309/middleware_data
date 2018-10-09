package com.icu.bigdata.utils

import java.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}


/**
  * sql
  *
  * @author limeng
  * @create 2018-08-07 下午6:42
  **/
object HiveUtil {
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

  /**
    * 插入hive,分区
    * @param sql
    * @param sqlModel
    * @param view
    */
  def addHive(sql:SQLContext,sqlModel:SqlModel1,view:String): Unit ={
    val year=sqlModel.year
    val month=sqlModel.month
    val day=sqlModel.day

    sql.sql(s"USE ${sqlModel.db}")
    sql.sql("SET hive.exec.dynamic.partition=true")
    sql.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    sql.sql(s"INSERT OVERWRITE TABLE ${sqlModel.tableName} PARTITION (year ='$year',month ='$month',day ='$day') SELECT * FROM $view")
  }

  /**
    * 全量插入hive
    * @param sql
    * @param sqlModel
    * @param view
    */
  def addHive(sql:SQLContext,sqlModel:SqlModel2,view:String): Unit ={

    sql.sql(s"USE ${sqlModel.db}")
    sql.sql("SET hive.exec.dynamic.partition=true")
    sql.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    sql.sql(s"INSERT OVERWRITE TABLE ${sqlModel.tableName}  SELECT * FROM $view")
  }

  /**
    * 查看表详情
    */
  def tableDetails(sql:SQLContext,sqlModel:SqlModel2): Array[Row] ={
    sql.sql(s"USE ${sqlModel.db}")
    sql.sql("SET hive.exec.dynamic.partition=true")
    sql.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    sql.sql(s"desc ${sqlModel.tableName}").collect()
  }

  /**
    * 转换数据
    * @param desc hive表详情
    * @param dataList mysql数据map集合
    * @param sql
    * @param view
    */
  def changeData(desc:Array[Row], dataList:List[Map[String, Any]], sql:SQLContext, view:String): Unit ={

    val fields=desc.map(x=>{ DataTypes.createStructField(x.getString(0), DataTypes.StringType, false)})
    val newData = new util.ArrayList[Row]

    //安照hive数据顺序
    dataList.foreach(f1 => {
      val value=desc.map(f2 => {
        var rows:String="null"
        val columnName = f2.getString(0)
        if (FilterUtils.isValidatePartitionLine(columnName)) {
          if (f1.get(columnName).mkString.isEmpty) {
            rows ="null"
          } else {
            rows =f1.get(columnName).mkString
          }
        }
        rows
      })
      newData.add(Row.fromSeq(value))
    })

    sql.createDataFrame(newData,DataTypes.createStructType(fields)).createOrReplaceTempView(view)

  }


}
case class SqlModel1(db:String,tableName:String,year:String,month:String,day:String)
case class SqlModel2(db:String,tableName:String)
