package com.whale.moby.qt.lib

import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer


object Utils extends java.io.Serializable {
  // 获取日期，Unix时间戳，并生成质检单号QIN （Quality inspection number）
  def getDateTimeQin(qtMethod: String): (Long, String, String) = {
    val localDateTime = LocalDateTime.now
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val unixTimestamp = java.sql.Timestamp.valueOf(localDateTime).getTime()
    val today = localDateTime.format(formatter)
    val qin  = qtMethod + "-" + unixTimestamp
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
    val minFormat = if(min.contains("-")){
      min.replace("-","")
    }else if(min.contains("/")){
      min.replace("/","")
    }else{
      min
    }

    val maxFormat = if(max.contains("-")){
      max.replace("-","")
    }else if(max.contains("/")){
      max.replace("/","")
    }else{
      max
    }

    (minFormat,maxFormat)
  }

  //得到对应得时间格式
  def getDateFormat(minFormat:String,maxFormat:String):(SimpleDateFormat,String)={
    var flag = "month"
    val format: SimpleDateFormat = if (minFormat.length == 6 && maxFormat.length == 6) {
      flag = "month"
      new SimpleDateFormat("yyyyMM")
    } else if (minFormat.length == 8 && maxFormat.length == 8) {
      flag = "day"
      new SimpleDateFormat("yyyyMMdd")
    } else {
      flag = "hour"
      new SimpleDateFormat("yyyyMMddHH")
    }
    (format,flag)
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


  //定义可变元组
  def xyTuple(): Unit ={

  }




  //结合上述生成-最大时间与最小时间,添加指定分区
  //格式：
  //示例1： 如果两个分区有关联：srv_name下有指定的srv_uri
  //srv_name:srv_uri%xxx=xxxx,xxxx,xxxx;
  def addOptPartitionParams(arrayList:ArrayBuffer[String],params:String): Unit ={
    //判定有多少分区
    val partitions : Array[String] = params.split(";")

    //
    val arrayDate = new ArrayBuffer[(String,String)]()



    //迭代遍历时间集合
    val it = partitions.iterator
    while(it.hasNext){

      val tuple: (String, String, String) = (it.next, "1", "1")
      arrayDate.append(tuple)



    }



  }



}
