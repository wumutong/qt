package com.whale.moby.qt.lib


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Main2Application {
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


    val dsNames = qtObject.split(" ")
    val (dsName1, dsName2) = (dsNames(0), dsNames(1))
    // val df = spark.sql(qtContent)
    // val df1 = spark.sql(qtContent)
    // val df2 = spark.sql(qtContent2)


    // 数据集转装载
    val df1 = if (scene == "scene1") spark.sql(qtContent) else spark.sql(qtContent).select(dsName1)
    val df2 = if (scene == "scene1") spark.sql(qtContent2) else spark.sql(qtContent).select(dsName2)


    // 获取QIN
    val dateTimeTuple = Utils.getDateTimeQin(qtMethod)
    // 组装样本数据
    val (dropDuplicateSQL1, dropDuplicateSQL2) = (DataKind.dropDuplicateSQL(df1, dsName1), DataKind.dropDuplicateSQL(df2, dsName2))
    val (dropDuplicateDF1, dropDuplicateDF2) = (spark.sql(dropDuplicateSQL1), spark.sql(dropDuplicateSQL2))
    val methodTuple = (qtCategory, qtMethod, scene)
    // 检测样本数据
    val checkedDF = matchMethod2(methodTuple, dropDuplicateDF1, dropDuplicateDF2, qtObject)
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
