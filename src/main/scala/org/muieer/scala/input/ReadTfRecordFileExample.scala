package org.muieer.scala.input

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField}
import org.muieer.scala.buildLocalSparkEnv

object ReadTfRecordFileExample {

  def main(args: Array[String]): Unit = {

    val Tuple2(spark, _) = buildLocalSparkEnv()
    run(spark, args)
  }

  def run(spark: SparkSession, args: Array[String]): Unit = {

    val df1 = spark.read.format("tfrecord").option("recordType", "Example").load(args(0))
    val map1 = getCleanedSchema(df1)
//    println("yhh")
//    df1.printSchema()

    val df2 = spark.read.format("tfrecord").option("recordType", "Example").load(args(1))
//    println("yhh")
//    df2.printSchema()
    val map2 = getCleanedSchema(df2)

    val diff = getSchemaDifference(map1, map2)

    println(
      s"""
         |${diff.size}
         |""".stripMargin)

    diff.foreach(line => println(line))


//    println(df1.schema)
  }



  // Compare relevant information
  def getSchemaDifference(schema1: Map[String, (DataType, Boolean)],
                          schema2: Map[String, (DataType, Boolean)]
                         ): Map[String, (Option[(DataType, Boolean)], Option[(DataType, Boolean)])] = {
    (schema1.keys ++ schema2.keys).
      map(_.toLowerCase).
      toList.distinct.
      flatMap { (columnName: String) =>
        val schema1FieldOpt: Option[(DataType, Boolean)] = schema1.get(columnName)
        val schema2FieldOpt: Option[(DataType, Boolean)] = schema2.get(columnName)

        if (schema1FieldOpt == schema2FieldOpt) None
        else Some(columnName -> (schema1FieldOpt, schema2FieldOpt))
      }.toMap
  }
}
