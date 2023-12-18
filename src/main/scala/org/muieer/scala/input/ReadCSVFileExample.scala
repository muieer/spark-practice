package org.muieer.scala.input

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.muieer.scala.buildLocalSparkEnv

object ReadCSVFileExample {

  def main(args: Array[String]): Unit = {

    val Tuple2(spark, _) = buildLocalSparkEnv()
    run(spark, args)
  }

  def run(spark: SparkSession, args: Array[String]): Unit = {

    val csvFilePath = args(0)
    val csvDataFrame = spark.read.option("header", "true").csv(csvFilePath)

    println(
      s"""
         |@muieer, csvDataFrame schema
         |${csvDataFrame.schema.toString()}
         |""".stripMargin)

    // 类型转化
    val parquetDataFrame = csvDataFrame
      .withColumn("feature_num", col("feature_num").cast(LongType))
      .withColumn("sample_num", col("sample_num").cast(LongType))
      .withColumn("cur_cover_rate", col("cur_cover_rate").cast(DoubleType))
      .withColumn("use_check_cover_rate", col("use_check_cover_rate").cast(DoubleType))
      .withColumn("change_rate", col("change_rate").cast(DoubleType))
      .withColumn("cur_fail_feature_count", col("cur_fail_feature_count").cast(IntegerType))
      .withColumn("max_fail_feature_count", col("max_fail_feature_count").cast(IntegerType))
      .withColumn("sample_rate", col("sample_rate").cast(DoubleType))
      .withColumn("alarm_threshold", col("alarm_threshold").cast(DoubleType))
      .withColumn("pass", col("pass").cast(BooleanType))
      .withColumn("alarm", col("alarm").cast(BooleanType))

    println(
      s"""
         |@muieer, parquetDataFrame schema
         |${parquetDataFrame.schema.toString()}
         |""".stripMargin)

    parquetDataFrame.repartition(1).write.mode(SaveMode.Overwrite).parquet(args(1))

  }
}
