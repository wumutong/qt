package com.whale.moby.qt.lib


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object MainApplication {
  def main(args: Array[String]): Unit = {
    // Starting Point
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf(true)
      .setAppName("Moby")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


    // 获取QIN
    val dateTimeTuple = Utils.getDateTimeQin(qtMethod)
    // 组装样本数据
    val methodTuple = (qtCategory, qtMethod, scene)
    val sampleSQL = matchSQL(methodTuple, spark, qtContent, dsDate)
    val sampleDF = spark.sql(sampleSQL)


    // 检测样本数据
    val checkedDF = matchMethod(methodTuple, sampleDF)
    // 格式化样本数据
    val jsonSQL = Utils.toJSON(checkedDF)
    // 封装明细数据
    checkedDF.createOrReplaceTempView("checked_detail")
    val detailSQL = """
                select
                md5('""" + dateTimeTuple._3 + """') qt_hc
                ,""" + jsonSQL + """
                ,'"""+ dateTimeTuple._1 + """' qt_version
                ,'""" + dateTimeTuple._2 + """' qt_date
                ,'""" + dsDate + """' ds_date
                from checked_detail a
                """
    val detailDF = spark.sql(detailSQL)
    // 明细数据入库
    writeMysql(detailDF, "qt_detail")


    // 质检整体评估
    val checkedSummary = overallAssessment(checkedDF)
    // 封装汇总数据
    val summarySql = """
                 select
                 md5('""" + dateTimeTuple._3 + """') qt_hc
                 ,"""" + qtCategory + """" qt_category
                 ,"""" + qtMethod+ """" qt_method
                 ,"""" + qtContent + """" qt_content
                 ,"""" + qtObject + """" qt_object
                 ,"""" + checkedSummary + """" qt_summary
                 ,"""" + dateTimeTuple._1 + """" qt_version
                 ,"""" + dateTimeTuple._2 + """" qt_date
                 ,"""" + dsDate + """" ds_date
                 """
    val summaryDF = spark.sql(summarySql)
    // 汇总数据入库
    writeMysql(summaryDF, "qt_summary")


    spark.stop()
  }
}

