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


    // 读取质检规则
    val rule_name = args(0)
    val sql = "(select rule_item_id from rule_item where rule_id = (select rule_id from rule where rule_name = '" + rule_name + "')) t"
    val ruleItemArray = readMySQL(spark, sql).collect.map(_.mkString(",").toInt)
    // 并行执行规则所包含的所有质检方法
    ruleItemArray.par.foreach(x => {

      RuleItem.handler(spark, x)

    })


    // Stop Point
    spark.stop()
  }
}
