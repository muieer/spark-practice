package org.muieer.scala.input

import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.muieer.scala.buildLocalSparkEnv
import org.muieer.scala.utils.DatasetSchemaUtil
import org.tensorflow.example.Example
import org.tensorflow.hadoop.io.TFRecordFileInputFormat

object ReadTfRecordFileExample {

  def main(args: Array[String]): Unit = {

    val Tuple2(spark, sc) = buildLocalSparkEnv()
    sparkSessionReadTfRecord(spark, args)
    sparkContextReadTfRecord(sc, args)
  }

  def sparkSessionReadTfRecord(spark: SparkSession, args: Array[String]): Unit = {

    val df1 = spark.read.format("tfrecord").option("recordType", "Example").load(args(0))
    df1.printSchema()

    val df2 = spark.read.format("tfrecord").option("recordType", "Example").load(args(1))
    df2.printSchema()

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
