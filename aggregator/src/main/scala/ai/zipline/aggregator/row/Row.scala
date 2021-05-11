package ai.zipline.aggregator.row

import ai.zipline.aggregator.windowing.TsUtils

trait Row {
  def get(index: Int): Any

  def ts: Long

  val length: Int

  def getAs[T](index: Int): T = get(index).asInstanceOf[T]

  def values: Array[Any] = (0 until length).map(get).toArray

}
