package ai.zipline.aggregator.windowing

import ai.zipline.api.Extensions.WindowOps
import ai.zipline.api._

case class BatchIr(collapsed: Array[Any], tailHops: HopsAggregator.IrMapType)
case class FinalBatchIr(collapsed: Array[Any], tailHops: HopsAggregator.OutputArrayType)

/**
  * Mutations processing starts with an end of the day snapshot FinalBatchIR.
  * On top of this FinalBatchIR mutations are processed.
  *
  *
  * update/merge/finalize are related to snapshot data. As such they follow the snapshot Schema
  * and aggregators.
  * However mutations come into play later in the group by and a finalized version of the snapshot
  * data is created to be processed with the mutations rows.
  * Since the dataframe inputs are aligned between mutations and snapshot (input) no additional schema is needed.
  *
  */
class SawtoothMutationAggregator(aggregations: Seq[Aggregation],
                                 inputSchema: Seq[(String, DataType)],
                                 resolution: Resolution = FiveMinuteResolution,
                                 tailBufferMillis: Long = new Window(2, TimeUnit.DAYS).millis)
    extends SawtoothAggregator(aggregations: Seq[Aggregation],
                               inputSchema: Seq[(String, DataType)],
                               resolution: Resolution) {

  val hopsAggregator = new HopsAggregatorBase(aggregations, inputSchema, resolution)

  def batchIrSchema: Array[(String, DataType)] = {
    val collapsedSchema = windowedAggregator.irSchema
    val hopFields = baseAggregator.irSchema :+ ("ts", LongType)
    Array("collapsedIr" -> StructType.from("WindowedIr", collapsedSchema),
          "tailHopIrs" -> ListType(ListType(StructType.from("HopIr", hopFields))))
  }

  def tailTs(batchEndTs: Long): Array[Option[Long]] =
    windowMappings.map { mapping => Option(mapping.aggregationPart.window).map { batchEndTs - _.millis } }

  def init: BatchIr = BatchIr(Array.fill(windowedAggregator.length)(null), hopsAggregator.init())

  def update(batchEndTs: Long, batchIr: BatchIr, row: Row): BatchIr = {
    val rowTs = row.ts
    val updatedHop = Array.fill(hopSizes.length)(false)
    var i = 0
    while (i < windowedAggregator.length) {
      if (batchEndTs > rowTs && tailTs(batchEndTs)(i).forall(rowTs > _)) { // relevant for the window
        if (tailTs(batchEndTs)(i).forall(rowTs >= _ + tailBufferMillis)) { // update collapsed part
          windowedAggregator.columnAggregators(i).update(batchIr.collapsed, row)
        } else { // update tailHops part
          val hopIndex = tailHopIndices(i)
          // eg., 7d, 8d windows shouldn't update the same 1hr tail hop twice
          // so update a hop only once
          if (!updatedHop(hopIndex)) {
            updatedHop.update(hopIndex, true)
            val hopStart = TsUtils.round(rowTs, hopSizes(hopIndex))
            val hopIr = batchIr.tailHops(hopIndex).computeIfAbsent(hopStart, hopsAggregator.javaBuildHop)
            baseAggregator.columnAggregators(baseIrIndices(i)).update(hopIr, row)
          }
        }
      }
      i += 1
    }
    batchIr
  }

  def merge(batchIr1: BatchIr, batchIr2: BatchIr): BatchIr =
    BatchIr(windowedAggregator.merge(batchIr1.collapsed, batchIr2.collapsed),
            hopsAggregator.merge(batchIr1.tailHops, batchIr2.tailHops))

  // Ready the snapshot aggregated data to be merged with mutations data.
  def finalizeSnapshot(batchIr: BatchIr): FinalBatchIr =
    FinalBatchIr(batchIr.collapsed, Option(batchIr.tailHops).map(hopsAggregator.toTimeSortedArray).orNull)

  /**
    * Go through the aggregators and update or delete the intermediate with the information of the row if relevant.
    * Useful for both online and mutations
    */
  def updateIr(ir: Array[Any], row: Row, queryTs: Long, hasReversal: Boolean = false) = {
    var i: Int = 0
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      val hopIndex = tailHopIndices(i)
      if (window == null || row.ts >= TsUtils.round(queryTs - window.millis, hopSizes(hopIndex))) {
        if (hasReversal && row.isBefore) {
          windowedAggregator(i).delete(ir, row)
        } else {
          windowedAggregator(i).update(ir, row)
        }
      }
      i += 1
    }
  }

  /**
    * Update the intermediate results with tail hops data from a FinalBatchIr.
    */
  def mergeTailHops(ir: Array[Any], queryTs: Long, batchEndTs: Long, batchIr: FinalBatchIr): Array[Any] = {
    var i: Int = 0
    while (i < windowedAggregator.length) {
      val window = windowMappings(i).aggregationPart.window
      if (window != null) { // no hops for unwindowed
        val hopIndex = tailHopIndices(i)
        val queryTail = TsUtils.round(queryTs - window.millis, hopSizes(hopIndex))
        val hopIrs = batchIr.tailHops(hopIndex)
        var idx: Int = 0
        while (idx < hopIrs.length) {
          val hopIr = hopIrs(idx)
          val hopStart = hopIr.last.asInstanceOf[Long]
          if ((batchEndTs - window.millis) + tailBufferMillis > hopStart && hopStart >= queryTail) {
            val merged = windowedAggregator(i).merge(ir(i), hopIr(baseIrIndices(i)))
            ir.update(i, merged)
          }
          idx += 1
        }
      }
      i += 1
    }
    ir
  }

  /**
    * Given aggregations FinalBatchIRs at the end of the Snapshot (batchEndTs) and mutation and query times,
    * determine the values at the query times for the aggregations.
    * This is pretty much a mix of online with extra work for multiple queries ts support.
    */
  def lambdaAggregateIrMany(batchEndTs: Long,
                            finalBatchIr: FinalBatchIr,
                            sortedInputs: Iterator[Row],
                            sortedEndTimes: Array[Long],
                            hasReversal: Boolean = false): Array[Array[Any]] = {
    if (sortedEndTimes == null) return null
    val batchIr = Option(finalBatchIr).getOrElse(finalizeSnapshot(init))
    val result = Array.fill[Array[Any]](sortedEndTimes.length)(windowedAggregator.clone(batchIr.collapsed))
    // Early exit. No mutations, no snapshot data.
    if (finalBatchIr == null && sortedInputs == null) return result
    val headRows = Option(sortedInputs).getOrElse(Array.empty[Row].iterator)
    // Go through mutations as they come. Then update all the aggregators that are in row.ts
    while (headRows.hasNext) {
      val row = headRows.next()
      val mutationTs = row.mutationTs
      var i: Int = 0
      while (i < sortedEndTimes.length) {
        val queryTs = sortedEndTimes(i)
        // Since mutationTs is always after rowTs then mutationTs before queryTs implies rowTs before query Ts as well
        if (batchEndTs <= mutationTs && mutationTs < queryTs) {
          updateIr(result(i), row, queryTs, hasReversal)
        }
        i += 1
      }
    }

    // Tail hops contain the window information that needs to be merged to the result.
    var i: Int = 0
    while (i < sortedEndTimes.length) {
      val queryTs = sortedEndTimes(i)
      mergeTailHops(result(i), queryTs, batchEndTs, batchIr)
      i += 1
    }
    result
  }
}
