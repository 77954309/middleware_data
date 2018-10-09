package com.icu.bigdata.route

import com.icu.bigdata.model.Data.{Hive1, Hive2, MySql1, MySql2}
import com.icu.bigdata.service.ToHive
import com.icu.bigdata.utils.{SparkUtil, TimeUtils}

/**
  * 导入hive
  *
  * @author limeng
  * @create 2018-09-03 下午1:56
  **/
object Import {
  def main(args: Array[String]): Unit = {
    println("driver,url,tableName,sql,location,dbName,tableName2,data(略)")
    //前面关系型数据库，后面hive
    //val Array(driver,url,tableName,sql,location,dbName,tableName2,data)=args
    val driver=args(0)
    val url=args(1)
    val tableName=args(2)
    val sql=args(3)
    val location=args(4)
    val dbName=args(5)
    val tableName2=args(6)


    val mysql=MySql1(driver,url,tableName,sql)

    if(args.length>7){
      //增量
      val data=args(7)
      val yearMonthDay=TimeUtils.strToYearMonthDay(data)
      val hive=Hive1(location,dbName,tableName2,yearMonthDay.year,yearMonthDay.month,yearMonthDay.day)
      ToHive.add(hive,mysql)
    }else{
      //全量
      val hive=Hive2(location,dbName,tableName2)
      ToHive.add(hive,mysql)
    }


  }
}
