package com.whale.moby.qt.lib

import org.apache.spark.sql.DataFrame
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat
import scala.collection.mutable.ArrayBuffer


object Utils extends java.io.Serializable {
  // 获取日期，Unix时间戳，并生成质检单号QIN （Quality inspection number）
  def getDateTimeQin(qtMethod: String, scene: String): (Long, String, String) = {
    val localDateTime = LocalDateTime.now
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val unixTimestamp = java.sql.Timestamp.valueOf(localDateTime).getTime()
    val today = localDateTime.format(formatter)
    val qin  = qtMethod + "-" + scene + "-" + unixTimestamp
    (unixTimestamp, today, qin)
  }
  // 转换样本数据为JSON格式
  def toJSON(checkedDF: DataFrame) = {
    val columns = checkedDF.drop("result").columns
    var jsonColumns: String = "to_json(struct("
    for (i <- 0 until columns.length){
      jsonColumns += columns(i) + ","
    }
    val jsonSQL = jsonColumns.dropRight(1) + ")) qt_sample, result qt_result"
    jsonSQL
  }

  //改变时间格式
  def changeDateTimeFormat(min: String,max:String):(String,String)={
    val conMinMax = min+","+max

    val conMinMaxFoemat = if(conMinMax.contains("-")){
      conMinMax.replace("-","")
    }else if(min.contains("/")){
      conMinMax.replace("/","")
    }else{
      conMinMax
    }
    (conMinMaxFoemat.split(",")(0),conMinMaxFoemat.split(",")(1))
  }

  //得到对应得时间格式
  def getDateFormat(minFormat:String,maxFormat:String):(SimpleDateFormat,String)={
    var flag = "month"
    var formatStr = "yyyyMM"

     if (minFormat.length == 8 && maxFormat.length == 8) {
      flag = "day"
      formatStr += "dd"
    } else {
      flag = "hour"
      formatStr += "ddHH"
    }

    (new SimpleDateFormat(formatStr),flag)
  }


  //获取相关时间顺序数组
  def getDateTimeSeq(min:String,max:String): ArrayBuffer[String] ={
    val arrayDate = new ArrayBuffer[String]()

    val flag  = Utils.getDateFormat(min,max)._2
    val dateFormat: SimpleDateFormat = Utils.getDateFormat(min, max)._1
    val dateBegin: Date = dateFormat.parse(min)
    val dateEnd: Date = dateFormat.parse(max)


    val calendarBegin: Calendar = Calendar.getInstance()
    val calendarEnd: Calendar = Calendar.getInstance()

    calendarBegin.setTime(dateBegin)
    calendarEnd.setTime(dateEnd)

    // 计算日期间隔毫秒数
    val diff = calendarEnd.getTimeInMillis() - calendarBegin.getTimeInMillis()


    if(flag=="month" || flag == "day"){
      // 计算日期间隔天数
      val diffDay = (diff / (1000 * 60 * 60 * 24)).toInt
      for (d <- 0 to diffDay) {
        // 日期转化成"yyyyMMdd"
        arrayDate.append(dateFormat.format(calendarBegin.getTime()))
        calendarBegin.add(Calendar.DATE, 1)
      }
    }else{
      // 计算日期相隔的小时
      val diffH = (diff / (1000 * 60 * 60)).toInt
      for (d <- 0 to diffH) {
        // 日期转化成"yyyyMMdd"
        arrayDate.append(dateFormat.format(calendarBegin.getTime()))
        calendarBegin.add(Calendar.HOUR, 1)
      }
    }
    arrayDate
  }

}

