package org.muieer

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

package object scala {

  private val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  var spark: SparkSession = _
  var sc: SparkContext = _

  def buildLocalSparkEnv(conf: SparkConf = new SparkConf()): (SparkSession, SparkContext) = {
    spark = SparkSession.builder()
      .appName("spark-practice-" + formatter.format(LocalDateTime.now()))
      .master("local")
      .config(conf)
      .getOrCreate()
    sc = spark.sparkContext
    (spark, sc)
  }

}
