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
    val columns = checkedDF.drop("result").drop("basis").columns
    var jsonColumns: String = "to_json(struct("
    for (i <- 0 until columns.length){
      jsonColumns += columns(i) + ","
    }
    val jsonSQL = jsonColumns.dropRight(1) + ")) qt_sample, result qt_result, basis judgment_basis"
    jsonSQL
  }

  //改变时间格式
  def changeDateTimeFormat(min: String,max:String):(String,String)={
    val conMinMax = min+","+max

    val conMinMaxFormat = if(conMinMax.contains("-")){
      conMinMax.replace("-","")
    }else if(min.contains("/")){
      conMinMax.replace("/","")
    }else{
      conMinMax
    }
    (conMinMaxFormat.split(",")(0),conMinMaxFormat.split(",")(1))
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
  def getDateTimeSeq(min:String,max:String,selfString : String = "1213=1231,13454,4654|jishias,dsaiojdasio"): ArrayBuffer[String] ={
    val arrayDate = new ArrayBuffer[String]()
    val dateFormat: SimpleDateFormat = Utils.getDateFormat(min, max)._1

    val calendarBegin: Calendar = Calendar.getInstance()
    val calendarEnd: Calendar = Calendar.getInstance()

    calendarBegin.setTime(dateFormat.parse(min))
    calendarEnd.setTime(dateFormat.parse(max))

    // 计算日期间隔毫秒数
    val diff = calendarEnd.getTimeInMillis() - calendarBegin.getTimeInMillis()

    if(Utils.getDateFormat(min,max)._2 =="month" || Utils.getDateFormat(min,max)._2 == "day"){
      // 计算日期间隔天数
      for (d <- 0 to (diff / (1000 * 60 * 60 * 24)).toInt) {

        // 日期转化成"yyyyMMdd"
        arrayDate.append(dateFormat.format(calendarBegin.getTime()))
        calendarBegin.add(Calendar.DATE, 1)
      }
    }else{
      // 计算日期相隔的小时
      for (d <- 0 to (diff / (1000 * 60 * 60)).toInt) {
        // 日期转化成"yyyyMMdd"
        arrayDate.append(dateFormat.format(calendarBegin.getTime()))
        calendarBegin.add(Calendar.HOUR, 1)
      }
    }
    arrayDate
  }


}

