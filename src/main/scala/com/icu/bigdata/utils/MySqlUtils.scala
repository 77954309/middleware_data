package com.icu.bigdata.utils


import com.icu.bigdata.model.Data.MySqlByTable
import org.apache.spark.sql.DataFrame
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

  /**
    * 创建临时表
    * @param df
    * @param sql
    */
  def addModel(df:DataFrame,sql:String): Unit ={
    df.sqlContext.sql(sql).createOrReplaceTempView("model")
  }

  /**
    * 查询所有
    * @param sql
    * @return
    */
  def queryAll(sql:String):List[Map[String, Any]] ={
    val maps: List[Map[String, Any]] = DB.readOnly(implicit session =>
      SQL(sql).map(_.toMap()).list().apply())
    maps
  }


}
