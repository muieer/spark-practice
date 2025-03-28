package org.muieer.scala.input

import org.apache.hadoop.io.{BytesWritable, Writable}
import org.apache.hadoop.mapred.{SequenceFileAsBinaryOutputFormat, SequenceFileInputFormat}
import org.apache.thrift.{TDeserializer, TSerializer}
import org.apache.thrift.protocol.TCompactProtocol
import org.muieer.scala.buildLocalSparkEnv

object ReadThriftFileExample {

  def main(args: Array[String]): Unit = {
    write(args)
  }

  def write(args: Array[String]): Unit = {
    val (spark, sc) = buildLocalSparkEnv()
    // 创建 10 个 AppUsageInfo
    val appUsageInfo1 = new AppUsageInfo()
    appUsageInfo1.setApp_id(1)
    val appUsageInfo2 = new AppUsageInfo()
    appUsageInfo2.setApp_id(2)
    val appUsageInfo3 = new AppUsageInfo()
    appUsageInfo3.setApp_id(3)
    val appUsageInfo4 = new AppUsageInfo()
    appUsageInfo4.setApp_id(4)
    val appUsageInfo5 = new AppUsageInfo()
    appUsageInfo5.setApp_id(5)
    val list = List(appUsageInfo1, appUsageInfo2, appUsageInfo3, appUsageInfo4, appUsageInfo5)

    val rdd = sc.makeRDD(list)
    rdd
      .map(app => {
        val ser = new TSerializer(new TCompactProtocol.Factory())
        val bytes = ser.serialize(app)
        (new BytesWritable(), new BytesWritable(bytes))
      })
      .saveAsHadoopFile[SequenceFileAsBinaryOutputFormat](args(0))
  }

  // 写数据的代码
  /*def saveAsSequenceFile(output: String,
                         codec: Class[_ <: CompressionCodec] = classOf[DefaultCodec],
                         protocol: String = "compact",
                         overwrite: Boolean = false): Unit = {
    if (overwrite) delete(rdd.sparkContext.hadoopConfiguration, output)
    val format = classOf[SequenceFileOutputFormat[Writable, Writable]]
    val jobConf = new JobConf(rdd.context.hadoopConfiguration)
    rdd
      .map { v =>
        val bytes =
          protocol match {
            case "compact" => ThriftSerde.serialize(v)
            case "binary" => ThriftSerde.serializeTB(v)
            case _ => throw new IllegalArgumentException(s"unsupported protocol: $protocol")
          }
        (new BytesWritable(), new BytesWritable(bytes))
      }
      .saveAsHadoopFile(
        output,
        classOf[BytesWritable],
        classOf[BytesWritable],
        format,
        jobConf,
        Some(codec)
      )
  }*/

  def read2():Unit = {
    val (spark, sc) = buildLocalSparkEnv()
    val path = ""

    sc.sequenceFile[Array[Byte], Array[Byte]](path)
      .map { case (key, value) =>
        println(s"key:${key.length}, value:${value.length}")
        val deserializer = new TDeserializer(new TCompactProtocol.Factory())
        val profile = new FeedsProfile()
        deserializer.deserialize(profile, value)
        println(profile)
        profile
      }
      .take(1)
      .foreach(println(_))
  }

  def read1():Unit = {
    val (spark, sc) = buildLocalSparkEnv()
    val path = ""

    sc.hadoopFile[Writable, Writable, SequenceFileInputFormat[Writable, Writable]](path)
      .map { case (key: BytesWritable, value: BytesWritable) =>
        println(s"key:${key.getBytes.length}, value:${value.getBytes.length}")
        val deserializer = new TDeserializer(new TCompactProtocol.Factory())
        val profile = new FeedsProfile()
        deserializer.deserialize(profile, value.getBytes)
        println(profile)
        profile
      }
      .take(1)
      .foreach(println(_))
  }
}