package ai.chronon.spark.stats

import ai.chronon.online.SparkConversions
import ai.chronon.online.AvroConversions
import ai.chronon.aggregator.row.StatsGenerator
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{PartitionRange, TableUtils}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.ScalaVersionSpecificCollectionsConverter

/**
  * Summary Job for daily upload of stats.
  * Leverage the stats module for computation per range.
  * Follow pattern of staging query for dividing long ranges into reasonable chunks.
  * Follow pattern of OOC for computing offline and uploading online as well.
  */
class SummaryJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {

  val tableUtils: TableUtils = TableUtils(session)
  private val dailyStatsTable = joinConf.metaData.dailyStatsOutputTable
  private val dailyStatsAvroTable = joinConf.metaData.dailyStatsUploadTable
  private val tableProps: Map[String, String] = Option(joinConf.metaData.tableProperties)
    .map(ScalaVersionSpecificCollectionsConverter.convertJavaMapToScala(_).toMap)
    .orNull

  def dailyRun(stepDays: Option[Int] = None, sample: Double = 0.1): Unit = {
    val unfilledRanges = tableUtils
      .unfilledRanges(dailyStatsTable, PartitionRange(null, endDate), Some(Seq(joinConf.metaData.outputTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) {
      println(s"No data to compute for $dailyStatsTable")
      return
    }
    unfilledRanges.foreach { computeRange =>
      println(s"Daily output statistics table $dailyStatsTable unfilled range: $computeRange")
      val stepRanges = stepDays.map(computeRange.steps).getOrElse(Seq(computeRange))
      println(s"Ranges to compute: ${stepRanges.map(_.toString).pretty}")
      // We are going to build the aggregator to denormalize sketches for hive.
      stepRanges.zipWithIndex.foreach {
        case (range, index) =>
          println(s"Computing range [${index + 1}/${stepRanges.size}]: $range")
          val inputDf = tableUtils.sql(s"""
               |SELECT *
               |FROM ${joinConf.metaData.outputTable}
               |WHERE ds BETWEEN '${range.start}' AND '${range.end}'
               |""".stripMargin)
          val stats = new StatsCompute(inputDf, joinConf.leftKeyCols, joinConf.metaData.nameToFilePath)
          val aggregator = StatsGenerator.buildAggregator(stats.metrics, StructType.from("selected", SparkConversions.toChrononSchema(stats.selectedDf.schema)))
          val summaryKvRdd = stats.dailySummary(aggregator, sample)
          if (joinConf.metaData.online) {
            // Store an Avro encoded KV Table and the schemas.
            val avroDf = summaryKvRdd.toAvroDf
            val schemas = Seq(summaryKvRdd.keyZSchema, summaryKvRdd.valueZSchema)
              .map(AvroConversions.fromChrononSchema(_).toString(true))
            val schemaKeys = Seq(Constants.StatsKeySchemaKey, Constants.StatsValueSchemaKey)
            val metaRows = schemaKeys.zip(schemas).map {
              case (k, schema) => Row(k.getBytes(Constants.UTF8), schema.getBytes(Constants.UTF8), k, schema, Constants.Partition.epochMillis(endDate))
            }
            val metaRdd = tableUtils.sparkSession.sparkContext.parallelize(metaRows)
            val metaDf = tableUtils.sparkSession.createDataFrame(metaRdd, avroDf.schema)
            avroDf
              .union(metaDf)
              .withColumn(Constants.PartitionColumn, lit(endDate))
              .save(dailyStatsAvroTable, tableProps)
          }
          stats
            .addDerivedMetrics(summaryKvRdd.toFlatDf, aggregator)
            .save(dailyStatsTable, tableProps)
          println(s"Finished range [${index + 1}/${stepRanges.size}].")
      }
    }
    println("Finished writing stats.")
  }
}
