package ai.zipline.spark

import org.apache.spark.sql.SparkSession

import java.io.File
import java.util.logging.Logger
import scala.reflect.io.Directory

object SparkSessionBuilder {

  val warehouseDir = new File("spark-warehouse")

  def build(name: String, local: Boolean = false): SparkSession = {
    if (local) {
      //required to run spark locally with hive support enabled - for sbt test
      System.setSecurityManager(null)
    }

    val baseBuilder = SparkSession
      .builder()
      .appName(name)
      .enableHiveSupport()
      .config("spark.sql.session.timeZone", "UTC")
      //otherwise overwrite will delete ALL partitions, not just the ones it touches
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "ai.zipline.spark.ZiplineKryoRegistrator")
      .config("spark.kryoserializer.buffer.max", "2000m")
      .config("spark.kryo.referenceTracking", "false")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      // Otherwise files left from deleting the table with the same name result in test failures
      .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
      .config("spark.sql.warehouse.dir", warehouseDir.getAbsolutePath)
      .config("spark.sql.catalogImplementation", "hive")

    val builder = if (local) {
      baseBuilder
      // use all threads - or the tests will be slow
        .master("local[*]")
        .config("spark.kryo.registrationRequired", "true")
        .config("spark.local.dir", s"/tmp/$name")
    } else {
      // hive jars need to be available on classpath - no needed for local testing
      baseBuilder
    }
    val spark = builder.getOrCreate()
    // disable log spam
    spark.sparkContext.setLogLevel("ERROR")
    Logger.getLogger("parquet.hadoop").setLevel(java.util.logging.Level.SEVERE)
    spark
  }
}
