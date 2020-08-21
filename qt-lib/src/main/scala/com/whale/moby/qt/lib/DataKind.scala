package com.whale.moby.qt.lib


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


object DataKind {
  // 质检方法
  // 结果集对比 - 任意类型
  private def compareCheck(seq: Seq[Any]) = if (seq.contains(null)) "不同" else "相同"
  val compareUDF = udf(compareCheck _)


  // 数据集对比样本送检
  // 场景1
  def compareSampleInspection(dropDuplicateDF1: DataFrame, dropDuplicateDF2: DataFrame, qtObject: String): DataFrame = {
    val dsNames = qtObject.split(" ")
    val (dsName1, dsName2) = (dsNames(0), dsNames(1))
    val columns = dropDuplicateDF1.drop("rn_" + dsName1).columns
    val joinedDF = dropDuplicateDF1.join(dropDuplicateDF2, columns, "full")
    val cols = array("rn_" + dsName1, "rn_" + dsName2)
    val checkedDF = joinedDF.withColumn("result", compareUDF(cols))
    checkedDF
  }
  // 场景2 -- 废弃
  /**
  def compareSampleInspection(dropDuplicateDF1: DataFrame, dropDuplicateDF2: DataFrame, qtObject: String, scene: String): DataFrame = {
    val dsNames = qtObject.split(" ")
    val (dsName1, dsName2) = (dsNames(0), dsNames(1))
    val joinedDF = dropDuplicateDF1.join(dropDuplicateDF2, dropDuplicateDF1(dsName1) === dropDuplicateDF2(dsName2), "full")
    val cols = array("rn_" + dsName1, "rn_" + dsName2)
    val checkedDF = joinedDF.withColumn("result", compareUDF(cols))
    checkedDF
  }
   */


  // 数据集对比SQL
  def dropDuplicateSQL(spark: SparkSession, qtContent: String, dsName: String) = {
    val df = spark.sql(qtContent)
    //val df = spark.sql(qtContent).select(dsName)
    val rnCol = df.columns(0)
    val w = Window.partitionBy().orderBy(rnCol)
    val rnDF = df.withColumn("rn", row_number().over(w))
    rnDF.createOrReplaceTempView(dsName)
    val columns = rnDF.columns
    var selectedColumns: String = "select "
    var groupColumns: String = " group by "
    for (i <- 0 until columns.length){
      selectedColumns += columns(i) + ","
      groupColumns += columns(i) + ","
    }
    val columnTurnedSQL = selectedColumns.dropRight(3) + "collect_set(rn) rn_" + dsName +  " from " + dsName + groupColumns.dropRight(4)
    columnTurnedSQL
  }


}
