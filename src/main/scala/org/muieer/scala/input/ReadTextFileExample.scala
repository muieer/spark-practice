package org.muieer.scala.input

import org.muieer.scala.buildLocalSparkEnv

object ReadTextFileExample {

  def main(args: Array[String]): Unit = {

    val (spark, sc) = buildLocalSparkEnv()
    println(sc.textFile(args(0)).count())

  }

}