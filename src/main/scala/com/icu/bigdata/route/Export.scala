package com.icu.bigdata.route

import com.icu.bigdata.model.Data.{Hive1, Hive2, MySql2}
import com.icu.bigdata.service.ToRelation
import com.icu.bigdata.utils.TimeUtils

/**
  * 导出:mysql
  *
  * @author limeng
  * @create 2018-09-03 下午2:02
  **/
object Export {
  def main(args: Array[String]): Unit = {
    println("location,dbName,tableName,data(略),driver,url,tableName2")
    //前面是hive 后面是关系型数据库
    //val Array(location,dbName,tableName,data,driver,url,tableName2)=args
    val location=args(0)
    val dbName=args(1)
    val tableName=args(2)
    var i=3
    val data=args(i)
    val yearMonthDay=TimeUtils.strToYearMonthDay(data)
    if(yearMonthDay!=null){
      val hive1=Hive1(location,dbName,tableName,yearMonthDay.year,yearMonthDay.month,yearMonthDay.day)
      val driver=args(i+1)
      val url=args(i+2)
      val tableName2=args(i+3)
      val mysql=MySql2(driver,url,tableName2)

      //增量
      ToRelation.incrementByTime(hive1,mysql)
    }else{
      val driver=args(i)
      val url=args(i+1)
      val tableName2=args(i+2)
      val mysql=MySql2(driver,url,tableName2)
      val hive2=Hive2(location,dbName,tableName)

      //全量
      ToRelation.add(hive2,mysql)
    }

  }
}
