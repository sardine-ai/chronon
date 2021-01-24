package ai.zipline.api

import org.apache.thrift.TBase
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import org.apache.thrift.TSerializer
import org.apache.thrift.protocol.TSimpleJSONProtocol

import scala.reflect.runtime.universe._
import scala.io.Source._

object ThriftJsonDecoder {
  val mapper = new ObjectMapper()
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromJsonStr[T <: TBase[_, _]: Manifest](jsonStr: String, check: Boolean = true, clazz: Class[T]): T = {
    val serializer = new TSerializer(new TSimpleJSONProtocol.Factory())
    val obj: T = mapper.readValue(jsonStr, clazz)
    if (check) {
      val whiteSpaceNormalizedInput = jsonStr.replaceAll("\\s", "")
      val reSerializedInput = new String(serializer.serialize(obj)).replaceAll("\\s", "")
      assert(
        whiteSpaceNormalizedInput == reSerializedInput,
        message = s"""
     Parsed Json object isn't reversible.
     Original JSON String:  $whiteSpaceNormalizedInput
     JSON produced by serializing object: $reSerializedInput"""
      )
    }
    obj
  }

  def fromJsonFile[T <: TBase[_, _]: Manifest](fileName: String, check: Boolean, clazz: Class[T]): T = {
    val src = fromFile(fileName)
    val jsonStr =
      try src.mkString
      finally src.close()
    val obj: T = fromJsonStr[T](jsonStr, check, clazz)
    obj
  }
}
