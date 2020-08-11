package com.whale.moby.qt


import org.apache.spark.sql.{DataFrame, SparkSession}
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
  // 必要参数配置
  val qtCategory: String = props.getProperty("qt.category")
  val qtMethod: String = props.getProperty("qt.method")
  val scene: String = props.getProperty("qt.scene")
  val qtContent: String = props.getProperty("qt.content")
  val dsDate: String = props.getProperty("qt.dsDate")
  val qtObject: String = props.getProperty("qt.object")
  // 附加参数配置
  val qtContent2: String = props.getProperty("qt.content2")
  val lowerLimit: String = props.getProperty("qt.lowerLimit")
  val upperLimit: String = props.getProperty("qt.upperLimit")

  // 数据写入MYSQL
  def writeMysql(df: DataFrame, dbtable: String) = {
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
  // 匹配/路由SQL
  def matchSQL(x: (String, String, String), spark: SparkSession, qtContent: String, dsDate: String) = x match {
    case ("BasicKind", "rangeCheck", "wave") => BasicKind.waveSQL(spark, qtContent, dsDate)
    case ("BasicKind", "nullCheck", "scene1") => BasicKind.nullSQL(qtContent, qtObject)

    case ("BasicKind", "duplicateDataCheck","scene1") => BasicKind.duplicateDataCheckSql(spark,qtContent)
    case ("BasicKind", "duplicateDataCheck","scene2") => BasicKind.duplicateDataCheckSql(spark,qtContent,qtObject)
    case ("BasicKind", "deletionDateCheck","scene1") => BasicKind.deletionDateCheckSql(spark,qtContent,qtObject)
    case _ => qtContent
  }
  // 匹配/路由方法 -- 基础类
  def matchMethod(x: (String, String, String), sampleDF: DataFrame, qtObject: String): DataFrame = x match {
    case ("BasicKind", "rangeCheck", "scene1") => BasicKind.rangeSampleInspection(sampleDF, qtObject, lowerLimit.toDouble, upperLimit.toDouble)
    case ("BasicKind", "rangeCheck", "scene2") => BasicKind.rangeSampleInspection(sampleDF, qtObject, lowerLimit, upperLimit)
    case ("BasicKind", "rangeCheck", "wave") => BasicKind.rangeSampleInspection(sampleDF, qtObject, lowerLimit, upperLimit, x._3)
    case ("BasicKind", "nullCheck", "scene1") => BasicKind.nullSampleInspection(sampleDF, qtObject)
    case ("BasicKind", "outliersCheck", "scene1") => BasicKind.outliersSampleInspection(sampleDF, qtObject)

    case ("BasicKind", "duplicateDataCheck","scene1") => BasicKind.duplicateDataInspection(sampleDF)
    case ("BasicKind", "duplicateDataCheck","scene2") => BasicKind.duplicateDataInspection(sampleDF)
    case ("BasicKind", "deletionDateCheck","scene1") => BasicKind.deletionDateInspection(sampleDF)
    case _ => null
  }
  // 匹配/路由方法 -- 数据类
  def matchMethod2(x: (String, String, String), dropDuplicateDF1: DataFrame, dropDuplicateDF2: DataFrame, qtObject: String): DataFrame = x match {
    case ("DataKind", "compareCheck", "scene1") => DataKind.compareSampleInspection(dropDuplicateDF1, dropDuplicateDF2, qtObject)
    case ("DataKind", "compareCheck", "scene2") => DataKind.compareSampleInspection(dropDuplicateDF1, dropDuplicateDF2, qtObject, x._3)
    case _ => null
  }
  // 整体评估  -- 不合格数量占总数量百分比高于10%则判定整体不通过。
  def overallAssessment(checkedDF: DataFrame) = {
    val unqualifiedNum = checkedDF.filter("result in ('非值域内数据','空值','异常值', '不同')").count.toDouble
    val totalNum = checkedDF.count.toDouble
    val unqualifiedPercent = unqualifiedNum/totalNum
    val checkedSummary = if (unqualifiedPercent <= 0.1 ) "质检通过" else "质检不通过"
    checkedSummary
  }

}




