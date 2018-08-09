package com.icu.bigdata.service

import com.icu.bigdata.model.Data._
import com.icu.bigdata.model.Data
import com.icu.bigdata.utils._
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.ConnectionPool

/**
  * HIVE导出到mysql
  * set hive.mapred.mode=nonstrict;关闭严格模式
  * @author limeng
  * @create 2018-08-07 下午3:23
  **/
object HiveToMysql {
  val logger: Logger = LoggerFactory.getLogger(HiveToMysql.getClass)

  /**
    *
    * @param hive
    * @param mySql
    */
    def incrementByTime(hive:Hive1,mySql:MySql2): Unit ={
      Data.callCase(mySql)
      val hiveSpark:SparkSession=Data.callCase(Hive3(hive.warehouseLocation,null)).asInstanceOf[Hive3].sparkSession
      val hiveSql=hiveSpark.sqlContext
      import hiveSql.implicits._

      val hiveDbName=hive.dbName
      val hiveTableName=hive.tableName
      val mySqlTableName=mySql.tableName

      val hivedF=SqlUtil.queryByHiveToPartition(hiveSql,SqlModel1(hiveDbName,hiveTableName,hive.year,hive.month,hive.day))
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
      val hiveSpark:SparkSession=Data.callCase(Hive3(hive.warehouseLocation,null)).asInstanceOf[Hive3].sparkSession
      val hiveSql=hiveSpark.sqlContext
      import hiveSql.implicits._

      val hiveDbName=hive.dbName
      val hiveTableName=hive.tableName
      val mySqlTableName=mySql.tableName

      val hivedF=SqlUtil.queryByHive(hiveSql,SqlModel2(hiveDbName,hiveTableName))
      val columns=hivedF.columns.filter(f=>FilterUtils.isValidatePartitionLine(f.toString)).mkString(",")
      MySqlUtils.batchSave(MySqlByTable(mySqlTableName,columns,hivedF.collect()))

      logger.info("add:hive"+hive+" mySql:"+mySql)
      ConnectionPool.closeAll
      hiveSpark.stop()
    }
}

