package com.whale.moby.qt.rule


import com.alibaba.fastjson.JSON
import com.whale.moby.qt.lib._
import org.apache.spark.sql.SparkSession


object RuleItem {


  def handler(spark: SparkSession, rule_item_id : Int) = {
    // 读取动态参数
    val ruleItemDF = readMySQL(spark, "rule_item")
    val jsonTake = ruleItemDF.filter("rule_item_id = " + rule_item_id).select("method_text").take(1)(0)(0).toString
    val json = JSON.parseObject(jsonTake)
    // 必要参数
    val qtCategory = json.getString("qtCategory")
    val qtMethod = json.getString("qtMethod")
    val scene = json.getString("scene")
    val qtContent = json.getString("qtContent")
    val qtObject = json.getString("qtObject")
    val dsDate = json.getString("dsDate")
    // 附加参数
    var lowerLimit: String = "default"
    var upperLimit: String = "default"
    var qtContent2: String = "default"
    if (qtMethod == "compareCheck" && scene == "scene1") {
      qtContent2 = json.getString("qtContent2")
    } else if (qtMethod == "rangeCheck") {
      lowerLimit = json.getString("lowerLimit")
      upperLimit = json.getString("upperLimit")
    }


    // 获取QIN
    val dateTimeTuple = Utils.getDateTimeQin(qtMethod, scene)
    // 组装样本数据
    val methodTuple = (qtCategory, qtMethod, scene)
    val sampleSQL = matchSQL(methodTuple, spark, qtContent, qtObject, dsDate, qtContent2)
    val sampleDF = (spark.sql(sampleSQL._1), spark.sql(sampleSQL._2))


    // 检测样本数据
    val checkedDF = if (qtCategory == "BasicKind") matchMethod(methodTuple, sampleDF._1, qtObject, lowerLimit, upperLimit) else matchMethod2(methodTuple, sampleDF._1, sampleDF._2, qtObject)
    // 格式化样本数据
    val jsonSQL = Utils.toJSON(checkedDF)
    // 封装明细数据
    checkedDF.createOrReplaceTempView("checked_detail_" + rule_item_id)
    val detailSQL = """
                select
                md5('""" + dateTimeTuple._3 + """') qt_hc
                ,""" + jsonSQL + """
                ,'"""+ dateTimeTuple._1 + """' qt_version
                ,'""" + dateTimeTuple._2 + """' qt_date
                ,'""" + dsDate + """' ds_date
                from checked_detail_""" + rule_item_id
    //todo
    println(detailSQL+"------执行该sql")

    val detailDF = spark.sql(detailSQL)
    // 明细数据入库
    writeMySQL(detailDF, "qt_detail")


    // 质检整体评估
    val checkedSummary = overallAssessment(checkedDF)
    // 封装汇总数据
    val summarySql = """
                 select
                 md5('""" + dateTimeTuple._3 + """') qt_hc
                 ,"""" + qtCategory + """" qt_category
                 ,"""" + qtMethod + """" qt_method
                 ,"""" + scene + """" qt_scene
                 ,"""" + qtContent + """" qt_content
                 ,"""" + qtObject + """" qt_object
                 ,"""" + checkedSummary + """" qt_summary
                 ,"""" + dateTimeTuple._1 + """" qt_version
                 ,"""" + dateTimeTuple._2 + """" qt_date
                 ,"""" + dsDate + """" ds_date
                 """

    println(summarySql+"---执行该sql")

    val summaryDF = spark.sql(summarySql)
    // 汇总数据入库
    writeMySQL(summaryDF, "qt_summary")
  }


}
