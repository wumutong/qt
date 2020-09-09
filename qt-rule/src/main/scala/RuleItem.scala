package com.whale.moby.qt.rule


import org.apache.spark.sql.SparkSession
import com.alibaba.fastjson.JSON
import com.whale.moby.qt.lib._


object RuleItem {


  def handler(spark: SparkSession, ruleName: String, ruleItemId : Int, startDate: String, endDate: String) = {
    // 读取动态参数
    val ruleItemDF = readMySQL(spark, "rule_item")
    val jsonTake = ruleItemDF.filter("rule_item_id = " + ruleItemId).select("method_text").take(1)(0)(0).toString
    val json = JSON.parseObject(jsonTake)
    // 必要参数
    val qtCategory = json.getString("qtCategory")
    val qtMethod = json.getString("qtMethod")
    val scene = json.getString("scene")
    val qtContent = json.getString("qtContent")
    val qtObject = json.getString("qtObject")
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
    val sampleSQL = matchSQL(methodTuple, spark, qtContent, qtObject, startDate, endDate, qtContent2)
    val sampleDF = (spark.sql(sampleSQL._1), spark.sql(sampleSQL._2))


    // 检测样本数据
    val checkedDF = if (qtCategory == "BasicKind") matchMethod(methodTuple, sampleDF._1, qtObject, lowerLimit, upperLimit) else matchMethod2(methodTuple, sampleDF._1, sampleDF._2, qtObject)
    // 格式化样本数据
    val jsonSQL = Utils.toJSON(checkedDF)
    // 封装明细数据
    checkedDF.createOrReplaceTempView("checked_detail_" + ruleItemId)
    val detailSQL = """
                select
                md5('""" + dateTimeTuple._3 + """') qt_hc
                ,""" + jsonSQL + """
                ,'"""+ dateTimeTuple._1 + """' qt_version
                ,'""" + dateTimeTuple._2 + """' qt_date
                ,'""" + startDate + """' start_date
                ,'""" + endDate + """' end_date
                from checked_detail_""" + ruleItemId
    val detailDF = spark.sql(detailSQL)
    // 明细数据入库
    writeMySQL(detailDF, "qt_result")


    // 质检整体评估
    val checkedSummary = overallAssessment(checkedDF)
    // 封装汇总数据
    val summarySql = """
                 select
                 md5('""" + dateTimeTuple._3 + """') qt_hc
                 ,"""" + ruleName + """" qt_rule
                 ,"""" + qtCategory + """" qt_category
                 ,"""" + qtMethod + """" qt_method
                 ,"""" + scene + """" qt_scene
                 ,"""" + qtContent + """" qt_content
                 ,"""" + qtObject + """" qt_object
                 ,"""" + checkedSummary + """" qt_summary
                 ,"""" + dateTimeTuple._1 + """" qt_version
                 ,"""" + dateTimeTuple._2 + """" qt_date
                 """
    val summaryDF = spark.sql(summarySql)
    // 汇总数据入库
    writeMySQL(summaryDF, "qt_result_overview")
  }


}
