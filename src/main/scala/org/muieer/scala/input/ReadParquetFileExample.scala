package org.muieer.scala.input

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, upper}
import org.muieer.scala.buildLocalSparkEnv

object ReadParquetFileExample {

  var spark: SparkSession = _

  def main(args: Array[String]): Unit = {

    this.spark = buildLocalSparkEnv()._1
    run(args)
  }

  def run(args: Array[String]): Unit = {

    val dataFrame = spark.read.parquet(args(0))
    dataFrame.printSchema()
    dataFrame.take(10).foreach(println(_))

    // 在原表基础上添加新列
    val dataFrame1 = dataFrame.withColumn("feature_num_sub_100", col("feature_num") - 100)
    dataFrame1.printSchema()
    dataFrame1.take(10).foreach(println(_))

    // 在原表基础上添加新列
    val dataFrame2 = dataFrame.select(
      col("*"),
      col("feature_num").-(100).as("feature_num_sub_100").cast("int")
    )
    dataFrame2.printSchema()
    dataFrame2.take(10).foreach(println(_))

  }

}
