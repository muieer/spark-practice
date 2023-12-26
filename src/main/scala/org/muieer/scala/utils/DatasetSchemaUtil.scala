package org.muieer.scala.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DataType, StructField}

object DatasetSchemaUtil {

  def getCleanedSchema(df: DataFrame): Map[String, (DataType, Boolean)] = {
    df.schema.map { (structField: StructField) =>
      structField.name -> (structField.dataType, structField.nullable)
    }.toMap
  }

  def getSchemaDifference(df1: DataFrame, df2: DataFrame): Map[String, (Option[(DataType, Boolean)], Option[(DataType, Boolean)])] = {

    getCleanedSchema(df1)
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
