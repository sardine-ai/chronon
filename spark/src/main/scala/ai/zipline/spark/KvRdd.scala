package ai.zipline.spark

import ai.zipline.api
import ai.zipline.online.{AvroCodec, AvroConversions}
import ai.zipline.spark.Extensions._
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class KvRdd(data: RDD[(Array[Any], Array[Any])], keySchema: StructType, valueSchema: StructType)(implicit
    sparkSession: SparkSession) {

  val keyZSchema: api.StructType = keySchema.toZiplineSchema("Key")
  val valueZSchema: api.StructType = valueSchema.toZiplineSchema("Value")
  val flatSchema: StructType = StructType(keySchema ++ valueSchema)
  val flatZSchema: api.StructType = flatSchema.toZiplineSchema("Flat")

  def toAvroDf: DataFrame = {
    val rowSchema = StructType(
      Seq(
        StructField("key_bytes", BinaryType),
        StructField("value_bytes", BinaryType),
        StructField("key_json", StringType),
        StructField("value_json", StringType)
      )
    )
    def encodeBytes(schema: api.StructType): Any => Array[Byte] = {
      val codec: AvroCodec = new AvroCodec(AvroConversions.fromZiplineSchema(schema).toString(true));
      { data: Any =>
        val record = AvroConversions.fromZiplineRow(data, schema).asInstanceOf[GenericData.Record]
        val bytes = codec.encodeBinary(record)
        bytes
      }
    }

    def encodeJson(schema: api.StructType): Any => String = {
      val codec: AvroCodec = new AvroCodec(AvroConversions.fromZiplineSchema(schema).toString(true));
      { data: Any =>
        val record = AvroConversions.fromZiplineRow(data, schema).asInstanceOf[GenericData.Record]
        val bytes = codec.encodeJson(record)
        bytes
      }
    }

    println(s"""
         |key schema: 
         |  ${AvroConversions.fromZiplineSchema(keyZSchema).toString(true)}
         |value schema: 
         |  ${AvroConversions.fromZiplineSchema(valueZSchema).toString(true)}
         |""".stripMargin)
    val keyToBytes = encodeBytes(keyZSchema)
    val valueToBytes = encodeBytes(valueZSchema)
    val keyToJson = encodeJson(keyZSchema)
    val valueToJson = encodeJson(valueZSchema)

    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result: Array[Any] = Array(keyToBytes(keys), valueToBytes(values), keyToJson(keys), valueToJson(values))
        new GenericRow(result)
    }
    sparkSession.createDataFrame(rowRdd, rowSchema)
  }

  def toFlatDf: DataFrame = {
    val rowRdd: RDD[Row] = data.map {
      case (keys, values) =>
        val result = new Array[Any](keys.length + values.length)
        System.arraycopy(keys, 0, result, 0, keys.length)
        System.arraycopy(values, 0, result, keys.length, values.length)
        Conversions.toSparkRow(result, flatZSchema).asInstanceOf[GenericRow]
    }
    sparkSession.createDataFrame(rowRdd, flatSchema)
  }
}
