package com.icu.bigdata.route

import com.icu.bigdata.model.Data.{Hive1, Hive2, MySql2}
import com.icu.bigdata.service.HiveToMysql

/**
  * 导出mysql
  *
  * @author limeng
  * @create 2018-08-07 下午3:14
  **/
object ExportMySql {
  def main(args: Array[String]): Unit = {
    println("types batch add")
    val Array(types,location,dbName,tableName,year,month,day,url,user,password,tableName2)=args

    val hive1=Hive1(location,dbName,tableName,year,month,day)
    val mysql=MySql2(url,user,password,tableName2)
    val hive2=Hive2(location,dbName,tableName)

    types match {
      case "add" => HiveToMysql.incrementByTime(hive1,mysql)
      case "batch" => HiveToMysql.add(hive2,mysql)
    }


  }
}
