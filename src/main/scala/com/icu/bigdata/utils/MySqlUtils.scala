package com.icu.bigdata.utils

import java.sql.Timestamp

import com.icu.bigdata.model.Data.MySqlByTable
import org.joda.time.DateTime
import scalikejdbc._
import scalikejdbc.config._


/**
  *
  *
  * @author limeng
  * @create 2018-08-08 下午7:16
  **/
object MySqlUtils {
  /**
    * 批量插入
    * @param mySqlByTable
    */
  def batchSave(mySqlByTable:MySqlByTable) :Unit= {
    DB.localTx { implicit session =>
      val tableName=mySqlByTable.tableName
      val columns= mySqlByTable.columns
      val values=mySqlByTable.values

      values.foreach(f=>{
        val f1=f.toSeq.map(y=>{
          var result:Any=y.toString
          if(FilterUtils.isValidateTime(y.toString)){
            result="'"+TimeUtils.strToTimestamp(y.toString).toString+"'"
            println(result)
          }
          result
        })
        SQL(s"insert into $tableName ($columns) values(${f1.mkString(",")})").update().apply()
      })
    }
  }
}
