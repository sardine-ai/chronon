package ai.chronon.spark.test

import ai.chronon.api.{Builders, Constants}
import ai.chronon.spark._
import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class LabelJoinTest {

  val spark: SparkSession = SparkSessionBuilder.build("LabelJoinTest", local = true)

  private val namespace = "label_join"
  private val tableName = "test_label_join"
  spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
  private val labelDS = "2022-10-30"
  private val tableUtils = TableUtils(spark)

  private val viewsGroupBy = TestUtils.createViewsGroupBy(namespace, spark)
  private val labelGroupBy = TestUtils.createAttributesGroupBy(namespace, spark)
  private val left = viewsGroupBy.groupByConf.sources.get(0)

  @Test
  def testLabelJoin(): Unit = {
    val labelJoinConf = createTestLabelJoin(30, 20)
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin()
    println(" == Computed == ")
    computed.show()
    val expected = tableUtils.sql(s"""
                                     SELECT v.listing_id as listing,
                                        ts,
                                        m_guests,
                                        m_views,
                                        dim_bedrooms as listing_attributes_dim_bedrooms,
                                        dim_room_type as listing_attributes_dim_room_type,
                                        v.ds,
                                        a.ds as label_ds
                                     FROM label_join.listing_views as v
                                     LEFT OUTER JOIN label_join.listing_attributes as a
                                     ON v.listing_id = a.listing_id
                                     WHERE a.ds = '2022-10-30'""".stripMargin)
    println(" == Expected == ")
    expected.show()
    assertEquals(computed.count(), expected.count())
    assertEquals(computed.select("label_ds").first().get(0), labelDS)

    val diff = Comparison.sideBySide(computed,
                                     expected,
                                     List("listing",
                                          "ds",
                                          "label_ds",
                                          "m_guests",
                                          "m_views",
                                          "listing_attributes_dim_room_type",
                                          "listing_attributes_dim_bedrooms"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computed.count()}")
      println(s"Expected count: ${expected.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }
    assertEquals(0, diff.count())
  }

  @Test
  def testLabelDsDoesNotExist(): Unit = {
    val labelJoinConf = createTestLabelJoin(30, 20)
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_null_label_ds", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    // label ds does not exist in label table, labels should be null
    val runner = new LabelJoin(joinConf, tableUtils, "2022-11-01")
    val computed = runner.computeLabelJoin()
    println(" == Computed == ")
    computed.show()
    assertEquals(computed.select("label_ds").first().get(0), "2022-11-01")
    assertEquals(computed
      .select("listing_attributes_dim_room_type")
      .first()
      .get(0),
      null)
  }

  @Test
  def testLabelRefresh(): Unit = {
    val labelJoinConf = createTestLabelJoin(60, 20)
    val joinConf = Builders.Join(
      Builders.MetaData(name = "label_refresh", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )

    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin()
    println(" == Computed == ")
    computed.show()
    assertEquals(computed.count(), 6)
    val computedRows = computed.collect()
    // drop partition in middle to test hole logic
    tableUtils.dropPartitions(s"${namespace}.${tableName}",
                              Seq("2022-10-02"),
                              subPartitionFilters = Map(Constants.LabelPartitionColumn -> labelDS))

    val runner2 = new LabelJoin(joinConf, tableUtils, labelDS)
    val refreshed = runner2.computeLabelJoin()
    println(" == Refreshed == ")
    refreshed.show()
    assertEquals(refreshed.count(), 6)
    assertTrue(computedRows sameElements (refreshed.collect()))
  }

  @Test
  def testLabelEvolution(): Unit = {
    val labelJoinConf = createTestLabelJoin(30, 20, "listing_labels")
    val tableName = "label_evolution"
    val joinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoinConf
    )
    val runner = new LabelJoin(joinConf, tableUtils, labelDS)
    val computed = runner.computeLabelJoin()
    println(" == First Run == ")
    computed.show()
    assertEquals(computed.count(), 6)

    //add additional label column
    val updatedLabelGroupBy = TestUtils.createAttributesGroupByV2(namespace, spark, "listing_labels")
    val updatedLabelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = updatedLabelGroupBy.groupByConf)
      ),
      leftStartOffset = 35,
      leftEndOffset = 20
    )
    val updatedJoinConf = Builders.Join(
      Builders.MetaData(name = tableName, namespace = namespace, team = "chronon"),
      left,
      labelPart = updatedLabelJoin
    )
    val runner2 = new LabelJoin(updatedJoinConf, tableUtils, "2022-11-01")
    val updated = runner2.computeLabelJoin()
    println(" == Updated Run == ")
    updated.show()
    assertEquals(updated.count(), 12)
    assertEquals(updated.where(updated("label_ds") === "2022-11-01").count(), 6)
    // expected
    // 3|  5|     15|   1|    PRIVATE_ROOM|   NEW_HOST|2022-11-11|2022-10-03|
    assertEquals(updated
                   .where(updated("label_ds") === "2022-11-01" && updated("listing") === "3")
                   .select("listing_labels_dim_host_type")
                   .first()
                   .get(0),
                 "NEW_HOST")
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidSource(): Unit = {
    // Invalid left data model entities
    val labelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = 10,
      leftEndOffset = 3
    )
    val invalidLeft = labelGroupBy.groupByConf.sources.get(0)
    val invalidJoinConf = Builders.Join(
      Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon"),
      invalidLeft,
      labelPart = labelJoin
    )
    new LabelJoin(invalidJoinConf, tableUtils, labelDS).computeLabelJoin()
  }

  @Test(expected = classOf[AssertionError])
  def testLabelJoinInvalidGroupBy(): Unit = {
    // Invalid label data model
    val labelJoin = Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = viewsGroupBy.groupByConf)
      ),
      leftStartOffset = 10,
      leftEndOffset = 3
    )
    val joinConf = Builders.Join(
      Builders.MetaData(name = "test_invalid_label_join", namespace = namespace, team = "chronon"),
      left,
      labelPart = labelJoin
    )
    new LabelJoin(joinConf, tableUtils, labelDS).computeLabelJoin()
  }

  def createTestLabelJoin(startOffset: Int,
                          endOffset: Int,
                          groupByTableName: String = "listing_attributes"): ai.chronon.api.LabelPart = {
    val labelGroupBy = TestUtils.createAttributesGroupBy(namespace, spark, groupByTableName)
    Builders.LabelPart(
      labels = Seq(
        Builders.JoinPart(groupBy = labelGroupBy.groupByConf)
      ),
      leftStartOffset = startOffset,
      leftEndOffset = endOffset
    )
  }
}