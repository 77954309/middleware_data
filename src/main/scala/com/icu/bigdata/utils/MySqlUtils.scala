package com.icu.bigdata.utils

import java.sql.Timestamp

import com.icu.bigdata.model.Data.MySqlByTable
import com.icu.bigdata.service.HiveToMysql
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc._
import scalikejdbc.config._

import scala.collection.mutable.ListBuffer


/**
  *
  *
  * @author limeng
  * @create 2018-08-08 下午7:16
  **/
object MySqlUtils {
  val logger: Logger = LoggerFactory.getLogger(MySqlUtils.getClass)
  /**
    * 批量插入
    * @param mySqlByTable
    */
  def batchSave(mySqlByTable:MySqlByTable) :Unit= {
    DB.localTx { implicit session =>
      val tableName=mySqlByTable.tableName
      val columns= mySqlByTable.columns
      val values=mySqlByTable.values
      val ids = new ListBuffer[Long]
      values.foreach(f=>{
        val f1=f.toSeq.map(y=>{
         FilterUtils.changeDateTime(y.toString)
        })
        ids.append(SQL(s"insert into $tableName ($columns) values(${f1.mkString(",")})").updateAndReturnGeneratedKey().apply())
      })

      if(ids.isEmpty){
        logger.error("插入异常tableName:"+tableName)
      }
    }
  }
}
