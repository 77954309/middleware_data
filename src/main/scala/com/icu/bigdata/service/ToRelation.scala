package com.icu.bigdata.service

import com.icu.bigdata.model.Data
import com.icu.bigdata.model.Data._
import com.icu.bigdata.utils._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.ConnectionPool

/**
  * HIVE导出到关系数据库
  * set hive.mapred.mode=nonstrict;关闭严格模式
  * @author limeng
  * @create 2018-08-07 下午3:23
  **/
object ToRelation {
  val logger: Logger = LoggerFactory.getLogger(ToRelation.getClass)

  /**
    * 增量
    * @param hive
    * @param mySql
    */
    def incrementByTime(hive:Hive1,mySql:MySql2): Unit ={
      Data.callCase(mySql)
      println("incrementByTime:hive"+hive+" mySql:"+mySql)
      val hiveSpark:SparkSession=Data.callCase(Hive3(hive.warehouseLocation,null)).asInstanceOf[Hive3].sparkSession
      val hiveSql=hiveSpark.sqlContext

      val hiveDbName=hive.dbName
      val hiveTableName=hive.tableName
      val mySqlTableName=mySql.tableName

      val hivedF=HiveUtil.queryByHiveToPartition(hiveSql,SqlModel1(hiveDbName,hiveTableName,hive.year,hive.month,hive.day))
      val columns=hivedF.columns.filter(f=>FilterUtils.isValidatePartitionLine(f.toString)).mkString(",")
      MySqlUtils.batchSave(MySqlByTable(mySqlTableName,columns,hivedF.collect()))


      logger.info("incrementByTime:hive"+hive+" mySql:"+mySql)
      ConnectionPool.closeAll
      hiveSpark.stop()
    }

  /**
    * 批量增加所有数据
    * @param hive
    * @param mySql
    */
    def add(hive:Hive2,mySql:MySql2): Unit ={
      Data.callCase(mySql)
      println("add:hive"+hive+" mySql:"+mySql)
      val hiveSpark:SparkSession=Data.callCase(Hive3(hive.warehouseLocation,null)).asInstanceOf[Hive3].sparkSession
      val hiveSql=hiveSpark.sqlContext

      val hiveDbName=hive.dbName
      val hiveTableName=hive.tableName
      val mySqlTableName=mySql.tableName

      val hivedF=HiveUtil.queryByHive(hiveSql,SqlModel2(hiveDbName,hiveTableName))
      val columns=hivedF.columns.filter(f=>FilterUtils.isValidatePartitionLine(f.toString)).mkString(",")
      MySqlUtils.batchSave(MySqlByTable(mySqlTableName,columns,hivedF.collect()))

      logger.info("add:hive"+hive+" mySql:"+mySql)
      ConnectionPool.closeAll
      hiveSpark.stop()
    }
}

