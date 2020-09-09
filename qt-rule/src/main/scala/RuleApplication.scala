package com.whale.moby.qt.rule


import com.whale.moby.qt.lib.readMySQL
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object RuleApplication {
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


    // 质检规则参数初始化
    val ruleName = args(0)
    // 读取质检规则明细ID
    val ruleItemSQL = "(select rule_item_id from rule_item where rule_id = (select rule_id from rule where rule_name = '" + ruleName + "')) t1"
    val ruleItemArray = readMySQL(spark, ruleItemSQL).collect.map(_.mkString(",").toInt)
    // 读取质检规则日期参数
    val dateSQL = "(select start_date, end_date from date_parameter where rule_id = (select rule_id from rule where rule_name = '" + ruleName + "')) t2"
    val dateParameterArray = readMySQL(spark, dateSQL).rdd.map(row=> (row(0).toString, row(1).toString)).collect
    val (startDate, endDate) = if (dateParameterArray.isEmpty) ("20200101","99991231") else (dateParameterArray(0)._1, dateParameterArray(0)._2)
    spark.sql("""set startDate = """ + startDate)
    spark.sql("""set endDate = """ + endDate)

    // 并行执行规则所包含的所有质检方法
    ruleItemArray.par.foreach(x => {

      RuleItem.handler(spark, ruleName, x, startDate, endDate)

    })


    // Stop Point
    spark.stop()
  }
}
