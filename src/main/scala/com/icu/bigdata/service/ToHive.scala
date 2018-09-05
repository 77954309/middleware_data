package com.icu.bigdata.service

import com.icu.bigdata.model.Data
import com.icu.bigdata.model.Data._
import com.icu.bigdata.utils._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc._

/**
  * 导入hive
  *
  * @author limeng
  * @create 2018-09-03 下午2:01
  **/
object ToHive {
  val logger: Logger = LoggerFactory.getLogger(ToHive.getClass)

  /**
    * 全量导入hive
    * @param hive
    * @param mySql
    */
  def add(hive:Hive2,mySql:MySql1): Unit ={
    Data.callCase(mySql)
    println("全量:hive"+hive+" mySql:"+mySql)
    val hiveSpark:SparkSession=Data.callCase(Hive3(hive.warehouseLocation,null)).asInstanceOf[Hive3].sparkSession
    val hiveSql=hiveSpark.sqlContext
    import hiveSql.implicits._
    import hiveSpark.implicits._

    //mysql视图
    val datas=MySqlUtils.queryAll(mySql.sql)
    val desc=HiveUtil.tableDetails(hiveSql,SqlModel2(hive.dbName,hive.tableName))

    //清洗数据
    HiveUtil.changeData(desc,datas,hiveSql,"model")
    HiveUtil.addHive(hiveSql,SqlModel2(hive.dbName,hive.tableName),"model")

    ConnectionPool.closeAll
    hiveSpark.stop()
  }


  /**
    *
    * hive分区批量增加
    * @param hive
    * @param mySql
    */
  def add(hive:Hive1,mySql:MySql1): Unit ={
    Data.callCase(mySql)
    println("分区增量:hive"+hive+" mySql:"+mySql)
    val hiveSpark:SparkSession=Data.callCase(Hive3(hive.warehouseLocation,null)).asInstanceOf[Hive3].sparkSession
    val hiveSql=hiveSpark.sqlContext
    import hiveSpark.implicits._
    import hiveSql.implicits._


    //mysql视图
    val datas=MySqlUtils.queryAll(mySql.sql)
    val desc=HiveUtil.tableDetails(hiveSql,SqlModel2(hive.dbName,hive.tableName))

    HiveUtil.changeData(desc,datas,hiveSql,"model")
    HiveUtil.addHive(hiveSql,SqlModel1(hive.dbName,hive.tableName,hive.year,hive.month,hive.day),"model")


    ConnectionPool.closeAll
    hiveSpark.stop()
  }

}
