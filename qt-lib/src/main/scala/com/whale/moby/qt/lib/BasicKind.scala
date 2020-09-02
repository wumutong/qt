package com.whale.moby.qt.lib


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer
import javax.script.ScriptEngineManager
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ArrayBuffer


object BasicKind extends java.io.Serializable {
  /**
   * 质检方法
   */
  // 值域检查 - 数值型
  private def rangeCheck(minValue: Double, maxValue: Double, seq: Seq[Double]) = {
    val resultList = ListBuffer[String]()
    for (i <- seq){
      val result = if (i >= minValue && i <= maxValue) "值域内数据" else "非值域内数据"
      resultList += result
    }
    val finalResult = if (resultList.contains("非值域内数据")) "值域检测不通过" else "值域检测通过"
    finalResult
  }
  val rangeCheckUDF = udf(rangeCheck _)
  // 空值检测 - 布尔型
  private def nullCheck(x: Boolean) = {
    val checkResult = if (x) "空值" else "非空值"
    checkResult
  }
  val nullCheckUDF = udf(nullCheck _)
  // 异常值检测 - 数值型
  private def outliersCheck(x: Double, Q1: Double, Q3: Double) = {
    val IQR = Q3 - Q1
    val checkResult = if (x < Q1 - 1.5 * IQR | x > Q3 + 1.5 * IQR) "异常值" else "非异常值"
    checkResult
  }
  val outliersCheckUDF = udf(outliersCheck _)
  // 重复值检测 - 字符型
  private def DuplicateDataCheck(y: String): String = {
    val checkResult = if (y == "1") "不重复" else "重复"
    checkResult
  }
  val uplicateDataCheckUdf = udf(DuplicateDataCheck _)
  // 缺失值检测 - 字符型
  private def DeletionDataCheck(y:String):String = {
    val checkResult = if (y == "0") "缺失" else "不缺失"
    checkResult
  }
  val deletionDataCheck = udf(DeletionDataCheck _)


  // 线性代数 - 表达式计算
  private def computeExpression(x: Double, expression: String) = {
    // 重组表达式
    val xExpression = expression.trim.drop(3)
    val pattern = """x""".r
    val reExpression = pattern.replaceFirstIn(xExpression,x.toString)
    // 执行计算
    val manager = new ScriptEngineManager
    val engine = manager.getEngineByName("javascript")
    val computeResult = engine.eval(reExpression)
    computeResult.toString.toDouble
  }
  val computeExpressionUDF = udf(computeExpression _)


  /**
   * 样本送检
   * 包含多种质检方法送检过程，每种质检方法包含多个场景
   */
  // 值域样本送检
  // 场景1
  def rangeSampleInspection(sampleDF: DataFrame, qtObject: String, lowerLimit: Double, upperLimit: Double): DataFrame = {
    // 预检处理
    // 检测样本数据
    val cols = qtObject.split(" ")
    val arrCols = array(cols map col: _*)
    val checkedDF = sampleDF.withColumn("result", rangeCheckUDF(lit(lowerLimit),lit(upperLimit), arrCols)).withColumn("basis", judgmentUDF(col("result")))
    checkedDF
  }
  // 场景2
  def rangeSampleInspection(sampleDF: DataFrame, objectArray: Array[String], lowerLimit: String, upperLimit: String): DataFrame = {
    // 预检处理
    val y = objectArray(0)
    val x = objectArray(1)
    // 检测样本数据
    // 表达式计算
    val computedDF = sampleDF.withColumn("minvalue", computeExpressionUDF(col(x), lit(lowerLimit))).withColumn("maxvalue", computeExpressionUDF(col(x), lit(upperLimit)))
    // 值域检查
    val arrCols = array(y)
    val checkedDF = computedDF.withColumn("result", rangeCheckUDF(col("minvalue"), col("maxvalue"), arrCols)).drop("minvalue").drop("maxvalue").withColumn("basis", judgmentUDF(col("result")))
    checkedDF
  }
  // 场景3
  def rangeSampleInspection(sampleDF: DataFrame, qtObject: String, lowerLimit: String, upperLimit: String): DataFrame = {
    // 检测质检样本数据
    // 表达式计算
    val computedDF = sampleDF.withColumn("minvalue", computeExpressionUDF(col("x"), lit(lowerLimit))).withColumn("maxvalue", computeExpressionUDF(col("x"), lit(upperLimit)))
    // 值域检查
    val cols = qtObject.split(" ")
    val arrCols = array(cols map col: _*)
    val checkedDF = computedDF.withColumn("result", rangeCheckUDF(col("minvalue"), col("maxvalue"), arrCols)).drop("minvalue").drop("maxvalue").withColumn("basis", judgmentUDF(col("result")))
    checkedDF.drop("x")
  }

  // 空值样本送检
  // 场景1
  def nullSampleInspection(sampleDF: DataFrame, qtObject: String): DataFrame = {
    // 空值检查
    val checkedDF = sampleDF.withColumn("result", nullCheckUDF(col("null_check_" + qtObject))).drop("null_check_" + qtObject).withColumn("basis", judgmentUDF(col("result")))
    checkedDF
  }

  // 异常值样本送检
  // 场景1
  def outliersSampleInspection(sampleDF: DataFrame, qtObject: String): DataFrame = {
    // 计算四分位数
    val quartileArray = sampleDF.stat.approxQuantile(qtObject, Array(0.25, 0.5, 0.75), 0.05)
    val (q1, q3) = (quartileArray(0), quartileArray(2))
    // 异常值检查
    val checkedDF = sampleDF.withColumn("result", outliersCheckUDF(col(qtObject), lit(q1), lit(q3))).withColumn("basis", judgmentUDF(col("result")))
    checkedDF
  }

  // 重复值样本送检
  // 场景1+2 检查一行中全部的数据看是否有重复(或者检测一列中是否有重复的值)
  def duplicateDataSampleInspection(sampleDF: DataFrame,num:String="num"): DataFrame = {
    // 检测质检样本数据
    val computedDf: DataFrame = sampleDF.withColumn("result", BasicKind.uplicateDataCheckUdf(col(num))).withColumn("basis", judgmentUDF(col("result")))
    // 表达式计算
    computedDf
  }

  // 缺失值样本送检
  def deletionDateSampleInspection(sampleDF: DataFrame): DataFrame = {
    // 检测质检样本数据
    val computedDf: DataFrame = sampleDF.withColumn("result", BasicKind.deletionDataCheck(col("tag"))).withColumn("basis", judgmentUDF(col("result")))
    //todo
    println("创建DateFrame")
    // 表达式计算
    computedDf
  }


  /**
   * 特定场景处理
   */
  // 波动比SQL
  def waveSQL(spark: SparkSession, qtContent: String, qtObject: String, dsDate: String) = {
    // 改写SQL
    val currentDF = spark.sql(qtContent)
    val aDayAgo = (dsDate.toLong -1).toString
    val aDayAgoContent = qtContent.replaceAll(dsDate, aDayAgo)
    val aDayAgoDF = spark.sql(aDayAgoContent)
    // 预检处理
    val w = Window.partitionBy().orderBy(qtObject)
    val yDF = currentDF.withColumn("rn", row_number().over(w))
    val xDF = aDayAgoDF.withColumn("xrn", row_number().over(w)).withColumnRenamed(qtObject, "x").select("xrn","x")
    // 合并二天的样本数据
    val mergedDF = yDF.join(xDF, yDF("rn") === xDF("xrn"), "full")
    mergedDF.createOrReplaceTempView("merged_data")
    // 生成样本数据
    val columns = yDF.columns
    var selectedColumns: String = "x,"
    for (i <- 0 until columns.length){
      selectedColumns += columns(i) + ","
    }
    "select " + selectedColumns.dropRight(1) + " from merged_data"
  }

  // 空值判断SQL
  def nullSQL(qtContent: String, qtObject: String) = {
    val isnullStatement = ", isnull("  + qtObject + ") null_check_" + qtObject + " from"
    val sampleSQL = qtContent.replaceAll("from", isnullStatement)
    sampleSQL
  }

  //重复值重组Sql   Scene1 全部一行判定是否重复【除分区】
  def duplicateDataCheckSql(spark:SparkSession,qtContent:String):String={
    // 创建临时表
    spark.sql(qtContent).createOrReplaceTempView("meta_duplicateData1")

    val  RepeatSQL  = "select *,count(1) over(partition by concat(*)) as num from meta_duplicateData1"
    RepeatSQL
  }

  //重复值重组Sql  Scene2 判定指定一行 是否重复
  def duplicateDataCheckSql(spark:SparkSession,qtContent:String,qtObject:String):String={
    // 创建临时表
    spark.sql(qtContent).createOrReplaceTempView("meta_duplicateData2")

    val  RepeatSQL  = "select *,count(1) over (partition by "+ qtObject +") as num from meta_duplicateData2"
    RepeatSQL
  }

  //缺失重组Sql  Scene指定一列分区进行判断
  def deletionDateCheckSql(spark:SparkSession,qtContent:String,qtObject:String):String={
    // 使用改写sql 创建临时表，生成指定的最大时间 和最小时间
    spark.sql(qtContent).createOrReplaceTempView("meta_deletionDate")
    val RepeatDF: DataFrame = spark.sql( "select min("+qtObject+") as mins,max("+qtObject+") as maxs from meta_deletionDate")

    //获取场景3 单列时间的最大时间，最小时间
    val stringsMin: Array[String] = RepeatDF.select("mins").collect().map(_ (0).toString)
    val mins: String = stringsMin(0)
    val stringsMax: Array[String] = RepeatDF.select("maxs").collect().map(_ (0).toString)
    val maxs: String = stringsMax(0)

    //todo
    println(mins+"----"+maxs)

    //创建相关基列列表
    val dateTuple: ArrayBuffer[(Int, Int)] = Utils.getDateTimeSeq(Utils.changeDateTimeFormat(mins,maxs)._1, Utils.changeDateTimeFormat(mins,maxs)._2).map(x => {
      (x.toInt, 1)
    })
    //创建 针对基列的 临时表
    spark.createDataFrame(dateTuple).toDF("dateTag", "tags").createOrReplaceTempView("baseTable")
    //判定sql  如果 tags=0 即该天为缺失
    val marginSql: String =
      "select step1.dateTag as dateTag,count(step2.date2) as tag " +
        "from " +
        "(select dateTag from baseTable) step1 " +
        "left join (select distinct " + qtObject +" as date2 from meta_deletionDate) step2 " +
        "on step1.dateTag = step2.date2 " +
        "group by step1.dateTag"
    marginSql
  }



}

