package ai.chronon.spark.test.bootstrap

import ai.chronon.api.Builders.Derivation
import ai.chronon.api.Extensions._
import ai.chronon.api._
import ai.chronon.online.Fetcher.Request
import ai.chronon.online.MetadataStore
import ai.chronon.spark.Extensions.DataframeOps
import ai.chronon.spark.test.{MockApi, OnlineUtils, SchemaEvolutionUtils}
import ai.chronon.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.ScalaJavaConversions.JListOps
import scala.util.ScalaVersionSpecificCollectionsConverter

class DerivationTest {

  val spark: SparkSession = SparkSessionBuilder.build("DerivationTest", local = true)
  private val tableUtils = TableUtils(spark)
  private val today = tableUtils.partitionSpec.at(System.currentTimeMillis())

  @Test
  def testBootstrapToDerivations(): Unit = {
    val namespace = "test_derivations"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(Builders.JoinPart(groupBy = groupBy)),
      rowIds = Seq("request_id"),
      externalParts = Seq(
        Builders.ExternalPart(
          Builders.ExternalSource(
            metadata = Builders.MetaData(name = "payments_service"),
            keySchema = StructType(name = "keys", fields = Array(StructField("user", StringType))),
            valueSchema = StructType(name = "values", fields = Array(StructField("user_txn_count_15d", LongType)))
          )
        ),
        Builders.ExternalPart(
          Builders.ContextualSource(
            fields = Array(StructField("user_txn_count_30d", LongType))
          )
        )
      ),
      derivations = Seq(
        Builders.Derivation(
          name = "*"
        ),
        // contextual feature rename
        Builders.Derivation(
          name = "user_txn_count_30d",
          expression = "ext_contextual_user_txn_count_30d"
        ),
        // derivation based on one external field (rename)
        Builders.Derivation(
          name = "user_txn_count_15d",
          expression = "ext_payments_service_user_txn_count_15d"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_30d",
          expression = "unit_test_user_transactions_amount_dollars_sum_30d"
        ),
        // derivation based on one group by field (rename)
        Builders.Derivation(
          name = "user_amount_15d",
          expression = "unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        // derivation based on two group by fields
        Builders.Derivation(
          name = "user_amount_30d_minus_15d",
          expression =
            "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
        ),
        // derivation based on one group by field and one contextual field
        Builders.Derivation(
          name = "user_amount_avg_30d",
          expression = "1.0 * unit_test_user_transactions_amount_dollars_sum_30d / ext_contextual_user_txn_count_30d"
        ),
        // derivation based on one group by field and one external field
        Builders.Derivation(
          name = "user_amount_avg_15d",
          expression =
            "1.0 * unit_test_user_transactions_amount_dollars_sum_15d / ext_payments_service_user_txn_count_15d"
        )
      ),
      metaData = Builders.MetaData(name = "test.derivations_join", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(baseJoin, today, tableUtils)
    val outputDf = runner.computeJoin()

    assertTrue(
      outputDf.columns sameElements Array(
        "user",
        "request_id",
        "ts",
        "user_txn_count_30d",
        "user_txn_count_15d",
        "user_amount_30d",
        "user_amount_15d",
        "user_amount_30d_minus_15d",
        "user_amount_avg_30d",
        "user_amount_avg_15d",
        "ds"
      ))

    val leftTable = baseJoin.left.getEvents.table

    /* directly bootstrap a derived feature field */
    val diffBootstrapDf = spark
      .table(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d_minus_15d"),
        col("ds")
      )
      .sample(0.8)
    val diffBootstrapTable = s"$namespace.bootstrap_diff"
    diffBootstrapDf.save(diffBootstrapTable)
    val diffBootstrapRange = diffBootstrapDf.partitionRange
    val diffBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "user_amount_30d_minus_15d"),
        startPartition = diffBootstrapRange.start,
        endPartition = diffBootstrapRange.end
      ),
      table = diffBootstrapTable
    )

    /* bootstrap an external feature field such that it can be used in a downstream derivation */
    val externalBootstrapDf = spark
      .table(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("ext_payments_service_user_txn_count_15d"),
        col("ds")
      )
      .sample(0.8)
    val externalBootstrapTable = s"$namespace.bootstrap_external"
    externalBootstrapDf.save(externalBootstrapTable)
    val externalBootstrapRange = externalBootstrapDf.partitionRange
    val externalBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "ext_payments_service_user_txn_count_15d"),
        startPartition = externalBootstrapRange.start,
        endPartition = externalBootstrapRange.end
      ),
      table = externalBootstrapTable
    )

    /* bootstrap an contextual feature field such that it can be used in a downstream derivation */
    val contextualBootstrapDf = spark
      .table(leftTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_txn_count_30d"),
        col("ds")
      )
      .sample(0.8)
    val contextualBootstrapTable = s"$namespace.bootstrap_contextual"
    contextualBootstrapDf.save(contextualBootstrapTable)
    val contextualBootstrapRange = contextualBootstrapDf.partitionRange
    val contextualBootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        // bootstrap of contextual fields will against the keys but it should be propagated to the values as well
        selects = Builders.Selects("request_id", "user_txn_count_30d"),
        startPartition = contextualBootstrapRange.start,
        endPartition = contextualBootstrapRange.end
      ),
      table = contextualBootstrapTable
    )

    /* construct final boostrap join with 3 bootstrap parts */
    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.derivations_join_w_bootstrap")
    bootstrapJoin
      .setBootstrapParts(
        ScalaVersionSpecificCollectionsConverter.convertScalaSeqToJava(
          Seq(
            diffBootstrapPart,
            externalBootstrapPart,
            contextualBootstrapPart
          ))
      )

    val runner2 = new ai.chronon.spark.Join(bootstrapJoin, today, tableUtils)
    val computed = runner2.computeJoin()

    // Comparison
    val expected = outputDf
      .join(diffBootstrapDf,
            outputDf("request_id") <=> diffBootstrapDf("request_id") and outputDf("ds") <=> diffBootstrapDf("ds"),
            "left")
      .join(
        externalBootstrapDf,
        outputDf("request_id") <=> externalBootstrapDf("request_id") and outputDf("ds") <=> externalBootstrapDf("ds"),
        "left")
      .join(contextualBootstrapDf,
            outputDf("request_id") <=> contextualBootstrapDf("request_id") and outputDf("ds") <=> contextualBootstrapDf(
              "ds"),
            "left")
      .select(
        outputDf("user"),
        outputDf("request_id"),
        outputDf("ts"),
        contextualBootstrapDf("user_txn_count_30d"),
        externalBootstrapDf("ext_payments_service_user_txn_count_15d").as("user_txn_count_15d"),
        outputDf("user_amount_30d"),
        outputDf("user_amount_15d"),
        coalesce(diffBootstrapDf("user_amount_30d_minus_15d"), outputDf("user_amount_30d_minus_15d"))
          .as("user_amount_30d_minus_15d"),
        (outputDf("user_amount_30d") * lit(1.0) / contextualBootstrapDf("user_txn_count_30d"))
          .as("user_amount_avg_30d"),
        (outputDf("user_amount_15d") * lit(1.0) / externalBootstrapDf("ext_payments_service_user_txn_count_15d"))
          .as("user_amount_avg_15d"),
        outputDf("ds")
      )

    val diff = Comparison.sideBySide(computed, expected, List("request_id", "user", "ts", "ds"))
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
  def testBootstrapToDerivationsNoStar(): Unit = {
    val namespace = "test_derivations_no_star"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)

    val bootstrapDf = spark
      .table(queryTable)
      .select(
        col("request_id"),
        col("user"),
        col("ts"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("user_amount_30d_minus_15d"),
        col("ds")
      )
    val bootstrapTable = s"$namespace.bootstrap_table"
    bootstrapDf.save(bootstrapTable)
    val bootstrapPart = Builders.BootstrapPart(
      query = Builders.Query(
        selects = Builders.Selects("request_id", "user_amount_30d", "user_amount_30d_minus_15d")
      ),
      table = bootstrapTable
    )

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val joinConf = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query()
      ),
      joinParts = Seq(joinPart),
      derivations = Seq(
        Builders.Derivation(
          name = "user_amount_30d",
          expression = "unit_test_user_transactions_amount_dollars_sum_30d"
        ),
        Builders.Derivation(
          name = "user_amount_30d_minus_15d",
          expression =
            "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
        )
      ),
      bootstrapParts = Seq(
        bootstrapPart
      ),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.derivations_join_no_star", namespace = namespace, team = "chronon")
    )

    val runner = new ai.chronon.spark.Join(joinConf, today, tableUtils)
    val outputDf = runner.computeJoin()

    // assert that no computation happened for join part since all derivations have been bootstrapped
    assertFalse(tableUtils.tableExists(joinConf.partOutputTable(joinPart)))

    val diff = Comparison.sideBySide(outputDf, bootstrapDf, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${outputDf.count()}")
      println(s"Expected count: ${bootstrapDf.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }

  @Test
  def testLoggingNonStar(): Unit = {
    runLoggingTest("test_derivations_logging_non_star", wildcardSelection = false)
  }

  @Test
  def testLogging(): Unit = {
    runLoggingTest("test_derivations_logging", wildcardSelection = true)
  }

  private def runLoggingTest(namespace: String, wildcardSelection: Boolean): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")

    val groupBy = BootstrapUtils.buildGroupBy(namespace, spark)
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val endDs = spark.table(queryTable).select(max(tableUtils.partitionColumn)).head().getString(0)

    val joinPart = Builders.JoinPart(groupBy = groupBy)
    val baseJoin = Builders.Join(
      left = Builders.Source.events(
        table = queryTable,
        query = Builders.Query(
          startPartition = endDs
        )
      ),
      externalParts = Seq(
        Builders.ExternalPart(
          Builders.ContextualSource(
            Array(StructField("request_id", StringType))
          )
        )),
      joinParts = Seq(joinPart),
      derivations =
        (if (wildcardSelection) {
           Seq(Derivation("*", "*"))
         } else {
           Seq.empty
         }) :+ Builders.Derivation(
          name = "user_amount_30d_minus_15d",
          expression =
            "unit_test_user_transactions_amount_dollars_sum_30d - unit_test_user_transactions_amount_dollars_sum_15d"
        ),
      rowIds = Seq("request_id"),
      metaData = Builders.MetaData(name = "test.derivations_logging", namespace = namespace, team = "chronon")
    )

    val bootstrapJoin = baseJoin.deepCopy()
    bootstrapJoin.getMetaData.setName("test.derivations_logging.bootstrap")
    bootstrapJoin.setBootstrapParts(
      Seq(
        Builders.BootstrapPart(
          table = bootstrapJoin.metaData.loggedTable
        )
      ).toJava
    )

    // Init artifacts to run online fetching and logging
    val kvStore = OnlineUtils.buildInMemoryKVStore(namespace)
    val mockApi = new MockApi(() => kvStore, namespace)
    OnlineUtils.serve(tableUtils, kvStore, () => kvStore, namespace, endDs, groupBy)
    val fetcher = mockApi.buildFetcher(debug = true)

    val metadataStore = new MetadataStore(kvStore, timeoutMillis = 10000)
    kvStore.create(Constants.ChrononMetadataKey)
    metadataStore.putJoinConf(bootstrapJoin)

    val requests = spark
      .table(queryTable)
      .select("user", "request_id", "ts")
      .collect()
      .map { row =>
        val (user, requestId, ts) = (row.getLong(0), row.getString(1), row.getLong(2))
        Request(bootstrapJoin.metaData.nameToFilePath,
                Map(
                  "user" -> user,
                  "request_id" -> requestId
                ).asInstanceOf[Map[String, AnyRef]],
                atMillis = Some(ts))
      }
    val future = fetcher.fetchJoin(requests)
    val responses = Await.result(future, Duration.Inf).toArray

    // Populate log table
    val logs = mockApi.flushLoggedValues
    assertEquals(requests.length, responses.length)
    assertEquals(1 + requests.length, logs.length)
    mockApi
      .loggedValuesToDf(logs, spark)
      .save(mockApi.logTable, partitionColumns = Seq(tableUtils.partitionColumn, "name"))
    SchemaEvolutionUtils.runLogSchemaGroupBy(mockApi, today, endDs)
    val flattenerJob = new LogFlattenerJob(spark, bootstrapJoin, endDs, mockApi.logTable, mockApi.schemaTable)
    flattenerJob.buildLogTable()
    val logDf = spark.table(bootstrapJoin.metaData.loggedTable)

    // Verifies that logging is full regardless of select star
    val baseColumns = Seq(
      "unit_test_user_transactions_amount_dollars_sum_15d",
      "unit_test_user_transactions_amount_dollars_sum_30d"
    )
    assertTrue(baseColumns.forall(logDf.columns.contains))

    val bootstrapJoinJob = new ai.chronon.spark.Join(bootstrapJoin, endDs, tableUtils)
    val computedDf = bootstrapJoinJob.computeJoin()
    if (!wildcardSelection) {
      assertTrue(baseColumns.forall(c => !computedDf.columns.contains(c)))
    }

    // assert that no computation happened for join part since all derivations have been bootstrapped
    assertFalse(tableUtils.tableExists(bootstrapJoin.partOutputTable(joinPart)))

    val baseJoinJob = new ai.chronon.spark.Join(baseJoin, endDs, tableUtils)
    val baseDf = baseJoinJob.computeJoin()

    val expectedDf = JoinUtils
      .coalescedJoin(
        leftDf = logDf,
        rightDf = baseDf,
        keys = Seq("request_id", "user", "ts", "ds"),
        joinType = "right"
      )
      .drop("schema_hash")

    val diff = Comparison.sideBySide(computedDf, expectedDf, List("request_id", "user", "ts", "ds"))
    if (diff.count() > 0) {
      println(s"Actual count: ${computedDf.count()}")
      println(s"Expected count: ${expectedDf.count()}")
      println(s"Diff count: ${diff.count()}")
      println(s"diff result rows")
      diff.show()
    }

    assertEquals(0, diff.count())
  }

  @Test
  def testContextual(): Unit = {
    val namespace = "test_contextual"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $namespace")
    val queryTable = BootstrapUtils.buildQuery(namespace, spark)
    val bootstrapDf = spark
      .table(queryTable)
      .select(
        col("request_id"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("context_1"),
        (rand() * 30000)
          .cast(org.apache.spark.sql.types.LongType)
          .as("context_2"),
        col("ds")
      )
      .withColumn("ext_contextual_context_1", col("context_1"))
      .withColumn("ext_contextual_context_2", col("context_2"))

    val bootstrapTable = s"$namespace.bootstrap_table"
    bootstrapDf.save(bootstrapTable)

    def buildJoinConf(derivations: Seq[Derivation], name: String): ai.chronon.api.Join = {
      Builders.Join(
        left = Builders.Source.events(
          table = queryTable,
          query = Builders.Query()
        ),
        joinParts = Seq(),
        externalParts = Seq(
          Builders.ExternalPart(
            Builders.ContextualSource(
              fields = Array(
                StructField("context_1", LongType),
                StructField("context_2", LongType)
              )
            )
          )),
        derivations = derivations,
        // to simulate log-based bootstrap, and assumption is that logs will contain all columns
        bootstrapParts = Seq(Builders.BootstrapPart(table = bootstrapTable)),
        rowIds = Seq("request_id"),
        metaData = Builders.MetaData(name = name, namespace = namespace, team = "chronon")
      )
    }
    def getSchema(joinConf: ai.chronon.api.Join): Seq[String] = {
      val runner = new ai.chronon.spark.Join(joinConf, today, tableUtils)
      val outputDf = runner.computeJoin()
      outputDf.columns
    }

    /* when no derivations are present, we keep the values and discard the keys */
    val schema1 = getSchema(buildJoinConf(Seq(), "test_1"))
    assertFalse(schema1.contains("context_1"))
    assertTrue(schema1.contains("ext_contextual_context_1"))
    assertFalse(schema1.contains("context_2"))
    assertTrue(schema1.contains("ext_contextual_context_2"))


    /*
     * In order to keep the `key` format, use explicit rename derivation
     * Otherwise, in a * derivation, we keep only the values and discard the keys
     */
    val schema2 = getSchema(
      buildJoinConf(
        Seq(
          Builders.Derivation(
            name = "context_1",
            expression = "ext_contextual_context_1"
          ),
          Builders.Derivation(
            name = "*",
            expression = "*"
          )
        ),
        "test_2"
      ))

    assertTrue(schema2.contains("context_1"))
    assertFalse(schema2.contains("ext_contextual_context_1"))
    assertFalse(schema2.contains("context_2"))
    assertTrue(schema2.contains("ext_contextual_context_2"))

    /*
     * In order to keep the `key` format, use explicit rename derivation
     * Without the * derivation, the other columns are all discarded
     */
    val schema3 = getSchema(
      buildJoinConf(
        Seq(
          Builders.Derivation(
            name = "context_1",
            expression = "ext_contextual_context_1"
          )
        ),
        "test_3"
      ))

    assertTrue(schema3.contains("context_1"))
    assertFalse(schema3.contains("ext_contextual_context_1"))
    assertFalse(schema3.contains("context_2"))
    assertFalse(schema3.contains("ext_contextual_context_2"))


    /*
     * If we want to keep both format, select both format explicitly
     */
    val schema4 = getSchema(
      buildJoinConf(
        Seq(
          Builders.Derivation(
            name = "context_1",
            expression = "ext_contextual_context_1"
          ),
          Builders.Derivation(
            name = "ext_contextual_context_1",
            expression = "ext_contextual_context_1"
          )
        ),
        "test_4"
      ))

    assertTrue(schema4.contains("context_1"))
    assertTrue(schema4.contains("ext_contextual_context_1"))
    assertFalse(schema4.contains("context_2"))
    assertFalse(schema4.contains("ext_contextual_context_2"))
  }
}