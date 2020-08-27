package com.whale.moby.qt


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.io.FileInputStream
import java.util.Properties


package object lib {
  // 读取配置文件
  private val props = new Properties()
  props.load(new FileInputStream("application.properties"))
  // 服务参数配置
  val url: String = props.getProperty("mysql.url")
  val user: String = props.getProperty("mysql.user")
  val password: String = props.getProperty("mysql.password")


  // 读取MYSQL数据
  def readMySQL(spark: SparkSession, table: String): DataFrame = {
    spark.read.format("jdbc").options(
      Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url"-> url,
        "user"-> user,
        "password"-> password,
        "dbtable"-> table)).load()
  }
  // 写入MYSQL数据
  def writeMySQL(df: DataFrame, dbtable: String) = {
    df.write.mode("Append").format("jdbc").options(
      Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> url,
        "user" -> user,
        "password" -> password,
        "dbtable" -> dbtable,
        "batchsize" -> "2000",
        "truncate" -> "false")).save()
  }


  // 匹配/路由SQL -- 所有类
  def matchSQL(x: (String, String, String), spark: SparkSession, qtContent: String, qtObject: String, dsDate: String, qtContent2: String = "default"): (String, String) = x match {
    case ("BasicKind", "rangeCheck", "wave") => (BasicKind.waveSQL(spark, qtContent, qtObject, dsDate), qtContent)
    case ("BasicKind", "nullCheck", "scene1") => (BasicKind.nullSQL(qtContent, qtObject), qtContent)
    case ("DataKind", "compareCheck", "scene1") => (DataKind.dropDuplicateSQL(spark, qtContent, qtObject.split(" ")(0)), DataKind.dropDuplicateSQL(spark, qtContent2, qtObject.split(" ")(1)))
    // 作者：沐桐
    case ("BasicKind", "duplicateDataCheck","scene1") => (BasicKind.duplicateDataCheckSql(spark,qtContent), qtContent)
    case ("BasicKind", "duplicateDataCheck","scene2") => (BasicKind.duplicateDataCheckSql(spark,qtContent,qtObject), qtContent)
    case ("BasicKind", "deletionDateCheck","scene1") => (BasicKind.deletionDateCheckSql(spark,qtContent,qtObject) , qtContent)
    case _ => (qtContent, qtContent)
  }


  // 匹配/路由方法 -- 基础类
  def matchMethod(x: (String, String, String), sampleDF: DataFrame, qtObject: String, lowerLimit: String, upperLimit: String): DataFrame = x match {
    case ("BasicKind", "rangeCheck", "scene1") => BasicKind.rangeSampleInspection(sampleDF, qtObject, lowerLimit.toDouble, upperLimit.toDouble)
    case ("BasicKind", "rangeCheck", "scene2") => val objectArray: Array[String] = qtObject.split(" "); BasicKind.rangeSampleInspection(sampleDF, objectArray, lowerLimit, upperLimit)
    case ("BasicKind", "rangeCheck", "wave") => BasicKind.rangeSampleInspection(sampleDF, qtObject, lowerLimit, upperLimit)
    case ("BasicKind", "nullCheck", "scene1") => BasicKind.nullSampleInspection(sampleDF, qtObject)
    case ("BasicKind", "outliersCheck", "scene1") => BasicKind.outliersSampleInspection(sampleDF, qtObject)
    // 作者：沐桐
    case ("BasicKind", "duplicateDataCheck","scene1") => BasicKind.duplicateDataSampleInspection(sampleDF)
    case ("BasicKind", "duplicateDataCheck","scene2") => BasicKind.duplicateDataSampleInspection(sampleDF)
    case ("BasicKind", "deletionDateCheck","scene1") => BasicKind.deletionDateSampleInspection(sampleDF)
    case _ => null
  }
  // 匹配/路由方法 -- 数据类
  def matchMethod2(x: (String, String, String), dropDuplicateDF1: DataFrame, dropDuplicateDF2: DataFrame, qtObject: String): DataFrame = x match {
    case ("DataKind", "compareCheck", "scene1") => DataKind.compareSampleInspection(dropDuplicateDF1, dropDuplicateDF2, qtObject)
    case _ => null
  }

  // 判定依据
  def judgment(x: String): String = x match {
    case "值域检测不通过" => " 不在值域范围内!!！"
    case "空值" => "数据为NULL或NaN!!!"
    case "异常值" => " 超过了上四分位+1.5倍IQR距离/下四分位-1.5倍IQR距离!!!"
    case "不同" => "两个结果集当前行对应的数据不完全相同!!!"
    case "重复" => "结果集有重复!!!"
    case "缺失" => "结果集有缺失!!!"
    case _ => ""
  }
  val judgmentUDF = udf(judgment _)


  // 整体评估  -- 不合格数量占总数量百分比高于10%则判定整体不通过。
  def overallAssessment(checkedDF: DataFrame) = {
    val unqualifiedNum = checkedDF.filter("result in ('值域检测不通过','空值','异常值', '不同', '重复', '缺失')").count.toDouble
    val totalNum = checkedDF.count.toDouble
    val unqualifiedPercent = unqualifiedNum/totalNum
    val checkedSummary = if (unqualifiedPercent <= 0.1 ) "本次质检通过" else "本次质检不通过"
    checkedSummary
  }


}




