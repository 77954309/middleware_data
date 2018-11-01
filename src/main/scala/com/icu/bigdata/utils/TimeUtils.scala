package com.icu.bigdata.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.joda.time.DateTime

/**
  * 时间
  *
  * @author limeng
  * @create 2018-06-08 下午4:34
  **/
object TimeUtils {
  val month_map = Map("Jan" -> 1, "Feb" -> 2, "Mar"->3, "Apr"-> 4, "May"->5, "Jun"->6, "Jul"->7,
    "Aug"->8, "Sep"-> 9, "Oct"->10, "Nov"-> 11, "Dec"-> 12)

  val dateFormat=new SimpleDateFormat("yyyy-MM-dd")
  val dateFormat3=new SimpleDateFormat("yyyyMMdd")
  val dateFormat2=new SimpleDateFormat("yyyy.MM.dd")
  val simpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val calendar:Calendar=Calendar.getInstance()

  def apply(time: String) ={
    calendar.setTime(simpleDateFormat.parse(time))
    calendar.getTimeInMillis
  }

  def apply(time: Date) ={
    calendar.setTime(time)
    calendar.getTimeInMillis
  }

  /**
    * 获取日期yyyy-MM-dd
    * @param amount 天数
    * @return
    */
  def getCertainDayTimeStr(amount: Int): String ={
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    dateFormat.format(time)
  }

  /**
    * 获取日期yyyy.MM.dd
    * @param amount 天数
    * @return
    */
  def getCertainDayTimeStr2(amount: Int): String ={
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    dateFormat2.format(time)
  }
  /**
    * 获取日期 long
    * @param amount 天数
    * @return
    */
  def getCertainDayTime(amount: Int): Long ={
    calendar.add(Calendar.DATE, amount)
    val time = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -amount)
    time
  }

  /**
    * 获取年月日
    * @param amount
    * @return
    */
  def getYearMonthDay(amount: Int):YearMonthDay={
    calendar.add(Calendar.DATE, amount)
    val year=calendar.get(Calendar.YEAR).toString
    val month=calendar.get(Calendar.MONTH).+(1)
    val monthStr = {
      if(month < 10){
        "0"+month
      }else{
        month+""
      }
    }
    val day=calendar.get(Calendar.DATE)
    val dayStr = {
      if(day < 10){
        "0"+day
      }else{
        day+""
      }
    }
    calendar.add(Calendar.DATE, -amount)

    YearMonthDay(year,monthStr,dayStr)
  }

  def getYearMonth(amount: Int):YearMonthDay={
    calendar.add(Calendar.DATE, amount)
    val year=calendar.get(Calendar.YEAR).toString
    val month=calendar.get(Calendar.MONTH).+(1)
    val monthStr = {
      if(month < 10){
        "0"+month
      }else{
        month+""
      }
    }
    val dayStr = ""
    calendar.add(Calendar.DATE, -amount)

    YearMonthDay(year,monthStr,dayStr)
  }

  /**
    * 字符串转日期
    * @param line
    * @return
    */
  def strToDate(line:String):Date={
    dateFormat.parse(line)
  }

  /**
    * 字符串转日期
    * @param line
    * @return
    */
  def strToYearMonthDay(line:String):YearMonthDay={
    if(FilterUtils.isValidateDateByMonth(line)){
      val date=line+"-01"
      calendar.setTime(dateFormat.parse(date))
      this.getYearMonth(0)

    }else if(FilterUtils.isValidateDate(line)){
      calendar.setTime(dateFormat.parse(line))
      this.getYearMonthDay(0)
    }else{
      null
    }
  }

  /**
    * 字符串转日期
    * @param line
    * @return
    */
  def strToTimestamp(line:String):Timestamp={
    new Timestamp(DateTime.parse(line).toDate.getTime)
  }

  /**
    * 字符串转日期
    * @param line
    * @return
    */
  def strToTimestamp(line:Date):Timestamp={
    new Timestamp(line.getTime)
  }
  /**
    * long转换字符串
    * @param line
    * @return
    */
  def longToString(line:Long):String={
    val date=new Date(line)
    simpleDateFormat.format(date)
  }

}

/**
  * 年月日
  * @param year
  * @param month
  * @param day
  */
case  class YearMonthDay(year:String,month:String,day:String)

case class CurrentTime(startDateStr:String,endDateStr:String)