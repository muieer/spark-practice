package org.muieer.scala.input

import org.muieer.scala.{buildLocalSparkEnv, sc, spark}

object ReadCSVFileExample {

  def main(args: Array[String]): Unit = {

    val Tuple2(spark, _) = buildLocalSparkEnv()

    val csvFilePath = args(0)
    val dataFrame = spark.read.option("header", "true").csv(csvFilePath)

    dataFrame.printSchema()
    dataFrame
      .take(10).foreach(println(_))
  }

}
