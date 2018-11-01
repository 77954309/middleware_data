package com.icu.bigdata.utils



import scala.util.matching.Regex

/**
  * 过滤
  *
  * @author limeng
  * @create 2018-08-08 下午4:10
  **/
object FilterUtils {
  //2018-01-01  20180101  2018-01匹配三种格式
  val dateReg1:Regex="""^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$""".r

  val dateReg2:Regex="""^[1-9]\d{3}(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])$""".r

  val dateReg3:Regex="""^[1-9]\d{3}-(0[1-9]|1[0-2])$""".r
  //匹配数据库账号密码
  val userReg:Regex="""^[user=]{5}""".r
  val passwordReg:Regex="""^[password=]{9}""".r
  /**
    * 验证hive分区字段
    * @param line
    * @return
    */
    def isValidatePartitionLine(line:String):Boolean={
      if(line.equals("year") || line.equals("month") || line.equals("day") || line.startsWith("#")){
        false
      }else{
        true
      }
    }

  /**
    * 字符串转日期
    * @param line
    * @return
    */
  def changeDateTime(line:String):String={
    val options1 = dateReg1.findFirstMatchIn(line)
    val options2 = dateReg2.findFirstMatchIn(line)
    var result:String=line
    if(options1.nonEmpty){
      result="'"+TimeUtils.strToTimestamp(line).toString+"'"
    }else if(options2.nonEmpty){
      result="'"+TimeUtils.strToTimestamp(TimeUtils.dateFormat3.parse(line)).toString+"'"
    }
    result
  }

  /**
    * 验证数据账号
    */
   def isValidateUserName(line:String):Boolean ={
      val options1 =userReg.findFirstMatchIn(line)
      if(options1.nonEmpty){
        true
      }else{
        false
      }
   }

  /**
    * 验证数据密码
    */
  def isValidatePassword(line:String):Boolean ={
    val options1 =passwordReg.findFirstMatchIn(line)
    if(options1.nonEmpty){
      true
    }else{
      false
    }
  }


  /**
    * 验证日期 2018-10
    */
  def isValidateDateByMonth(line:String): Boolean ={
    val options1 = dateReg3.findFirstMatchIn(line)
    if(options1.nonEmpty){
      true
    }else{
      false
    }
  }

  /**
    * 验证日期
    */
  def isValidateDate(line:String): Boolean ={
    val options1 = dateReg1.findFirstMatchIn(line)
    if(options1.nonEmpty){
      true
    }else{
      false
    }
  }
}
