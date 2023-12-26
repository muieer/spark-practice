package org.muieer.scala.compute

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.muieer.scala.buildLocalSparkEnv

object DataFrameComputeExample {

  def main(args: Array[String]): Unit = {

    val Tuple2(spark, _) = buildLocalSparkEnv()
//    showAllData(spark, args)
    runTemp(spark, args)
  }

  def showAllData(spark: SparkSession, args: Array[String]): Unit = {

    val df = spark.read.parquet(args(0))
    df.schema
    df.sort(desc("cur_cover_rate")).collect().foreach(println(_))
  }

  def runTemp(spark: SparkSession, args: Array[String]): Unit = {

    var ds: Dataset[Row] = spark.read.parquet(args.head)
    args.tail.foreach(path => ds = ds.union(spark.read.parquet(path)))

    // 过滤校验不通过样本的校验不通过特征
    ds = ds.filter(!col("alarm") || col("pass"))
//    println(s"@muieer, ${ds.count()}")

    // 聚合
    ds = ds.groupBy("feature_name").agg(
      avg("cur_cover_rate").cast("double").as("history_avg_cover_rate"),
      min("use_check_cover_rate").cast("double").as("min_history_avg_cover_rate"),
      max("use_check_cover_rate").cast("double").as("max_history_avg_cover_rate"),
      min("sample_num").cast("long").as("history_min_sample_num"),
      max("sample_num").cast("long").as("history_max_sample_num")
    )
//    ds.printSchema()
//    println(s"@muieer, ds.groupBy count is ${ds.count()}")
      ds
      .sort(asc("history_avg_cover_rate"))
      .filter(col("history_avg_cover_rate") > 0.1)
      .take(10).foreach(println(_))
  }

  def run(spark: SparkSession, args: Array[String]): Unit = {

    var ds: Dataset[Row] = spark.read.parquet(args.head)
    args.tail.foreach(path => ds = ds.union(spark.read.parquet(path)))
    ds.persist()
    println(s"@muieer, ${ds.count()}")

    ds.groupBy("feature_name").agg(
      avg("cur_cover_rate").as("history_avg_cover_rate"),
      min("use_check_cover_rate").as("min_history_avg_cover_rate"),
      max("use_check_cover_rate").as("max_history_avg_cover_rate"),
      floor(avg("sample_num").as("avg_sample_num")),
      min("sample_num").as("min_sample_num"),
      max("sample_num").as("max_sample_num"),
    )
      .sort(asc("history_avg_cover_rate"))
      .filter(col("history_avg_cover_rate") > 0.1)
      .take(10).foreach(println(_))
  }

}