package org.muieer.scala.input

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, StringType, StructField, StructType}
import org.muieer.scala.buildLocalSparkEnv
import org.muieer.scala.utils.DatasetSchemaUtil
import org.tensorflow.example.Example
import org.tensorflow.hadoop.io.TFRecordFileInputFormat

object ReadTfRecordFileExample {

  val schema: StructType = StructType(Array(
    StructField("cityId", ArrayType(LongType, containsNull = true)),
    StructField("cvr", ArrayType(FloatType, containsNull = true)),
    StructField("ctr", ArrayType(FloatType, containsNull = true)),
    StructField("extraData", ArrayType(StringType))
  ))

  def main(args: Array[String]): Unit = {

    val Tuple2(spark, sc) = buildLocalSparkEnv()
    sparkSessionReadTfRecord(spark, args)
    sparkContextReadTfRecord(sc, args)
  }

  // 带 schema 和不带 schema 的区别很大，默认的 schema 解析逻辑有很大问题
  def sparkSessionReadTfRecord(spark: SparkSession, args: Array[String]): Unit = {

    val df1 = spark.read.schema(schema).format("tfrecord").option("recordType", "Example").load(args(0))
    df1.printSchema()
    df1.collect().foreach(println(_))

    val df2 = spark.read.schema(schema).format("tfrecord").option("recordType", "Example").load(args(1))
    df2.printSchema()
    df2.collect().foreach(println(_))

    println(DatasetSchemaUtil.getSchemaDifference(df1, df2))
  }

  def sparkContextReadTfRecord(sc: SparkContext, args: Array[String]): Unit = {

    sc.newAPIHadoopFile[BytesWritable, NullWritable, TFRecordFileInputFormat](args(0))
      .map { case (bytesWritable: BytesWritable, _) => Example.parseFrom(bytesWritable.getBytes)}
      .collect().foreach(println(_))

    sc.newAPIHadoopFile[BytesWritable, NullWritable, TFRecordFileInputFormat](args(1))
      .map { case (bytesWritable: BytesWritable, _) => Example.parseFrom(bytesWritable.getBytes) }
      .collect().foreach(println(_))
  }

}
