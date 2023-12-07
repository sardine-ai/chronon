/*
 *    Copyright (C) 2023 The Chronon Authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package ai.chronon.spark.stats

import org.slf4j.LoggerFactory
import ai.chronon.online.SparkConversions
import ai.chronon.aggregator.row.StatsGenerator
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.spark.Extensions._
import ai.chronon.spark.{JoinUtils, PartitionRange, TableUtils}
import org.apache.spark.sql.SparkSession

/**
  * Summary Job for daily upload of stats.
  * Leverage the stats module for computation per range.
  * Follow pattern of staging query for dividing long ranges into reasonable chunks.
  * Follow pattern of OOC for computing offline and uploading online as well.
  */
class SummaryJob(session: SparkSession, joinConf: Join, endDate: String) extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(getClass)

  val tableUtils: TableUtils = TableUtils(session)
  private val loggingStatsTable = joinConf.metaData.loggingStatsTable
  private val dailyStatsTable = joinConf.metaData.dailyStatsOutputTable
  private val tableProps: Map[String, String] = tableUtils.getTableProperties(joinConf.metaData.outputTable).orNull

  def basicStatsJob(inputTable: String,
                    outputTable: String,
                    columns: Option[Seq[String]],
                    stepDays: Option[Int] = None,
                    sample: Double = 0.1): Unit = {
    val uploadTable = joinConf.metaData.toUploadTable(outputTable)
    val backfillRequired = !JoinUtils.tablesToRecompute(joinConf, outputTable, tableUtils).isEmpty
    if (backfillRequired)
      Seq(outputTable, uploadTable).foreach(tableUtils.dropTableIfExists(_))
    val unfilledRanges = tableUtils
      .unfilledRanges(outputTable, PartitionRange(null, endDate)(tableUtils), Some(Seq(inputTable)))
      .getOrElse(Seq.empty)
    if (unfilledRanges.isEmpty) {
      logger.info(s"No data to compute for $outputTable")
      return
    }
    unfilledRanges.foreach { computeRange =>
      logger.info(s"Daily output statistics table $outputTable unfilled range: $computeRange")
      val stepRanges = stepDays.map(computeRange.steps).getOrElse(Seq(computeRange))
      logger.info(s"Ranges to compute: ${stepRanges.map(_.toString).pretty}")
      // We are going to build the aggregator to denormalize sketches for hive.
      stepRanges.zipWithIndex.foreach {
        case (range, index) =>
          logger.info(s"Computing range [${index + 1}/${stepRanges.size}]: $range")
          val joinOutputDf = tableUtils.sql(s"""
               |SELECT *
               |FROM ${inputTable}
               |WHERE ds BETWEEN '${range.start}' AND '${range.end}'
               |""".stripMargin)
          val inputDf = if (columns.isDefined) {
            val toSelect = columns.get
            joinOutputDf.select(toSelect.head, toSelect.tail: _*)
          } else joinOutputDf
          val stats = new StatsCompute(inputDf, joinConf.leftKeyCols, joinConf.metaData.nameToFilePath)
          val aggregator = StatsGenerator.buildAggregator(
            stats.metrics,
            StructType.from("selected", SparkConversions.toChrononSchema(stats.selectedDf.schema)))
          val summaryKvRdd = stats.dailySummary(aggregator, sample)
          // Build upload table for stats store.
          summaryKvRdd.toAvroDf
            .withTimeBasedColumn(tableUtils.partitionColumn)
            .save(uploadTable, tableProps)
          stats
            .addDerivedMetrics(summaryKvRdd.toFlatDf, aggregator)
            .save(outputTable, tableProps)
          logger.info(s"Finished range [${index + 1}/${stepRanges.size}].")
      }
    }
    logger.info("Finished writing stats.")
  }

  /**
    * Daily stats job for backfill output tables.
    * Filters contextual and external features.
    */
  def dailyRun(stepDays: Option[Int] = None, sample: Double = 0.1): Unit = {
    val outputSchema = tableUtils.getSchemaFromTable(joinConf.metaData.outputTable)
    val baseColumns = joinConf.leftKeyCols ++ joinConf.computedFeatureCols :+ tableUtils.partitionColumn
    val columns = if (outputSchema.map(_.name).contains(Constants.TimeColumn)) {
      baseColumns :+ Constants.TimeColumn
    } else {
      baseColumns
    }
    basicStatsJob(joinConf.metaData.outputTable, dailyStatsTable, Some(columns), stepDays, sample)
  }

  /**
    * Batch stats compute and upload for the logs
    * Does not filter contextual or external features.
    */
  def loggingRun(stepDays: Option[Int] = None, sample: Double = 0.1): Unit =
    basicStatsJob(joinConf.metaData.loggedTable, loggingStatsTable, None, stepDays, sample)
}
