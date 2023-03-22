package ai.chronon.spark

import ai.chronon.api
import ai.chronon.api.Extensions._
import ai.chronon.api.{Constants, ExternalPart, JoinPart, StructField}
import ai.chronon.online.SparkConversions
import ai.chronon.spark.Extensions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

import scala.collection.{Seq, immutable}
import scala.util.ScalaJavaConversions.ListOps

case class JoinPartMetadata(
    joinPart: JoinPart,
    keySchema: Array[StructField],
    valueSchema: Array[StructField],
    // list of derived fields that depend on any of the value fields in this join part
    requiringFields: Array[StructField]
)

case class ExternalPartMetadata(
    externalPart: ExternalPart,
    keySchema: Array[StructField],
    valueSchema: Array[StructField],
    // list of derived fields that depend on any of the value fields in this join part
    requiringFields: Array[StructField]
)

case class BootstrapInfo(
    joinConf: api.Join,
    // join parts enriched with expected output schema
    joinParts: Seq[JoinPartMetadata],
    // external parts enriched with expected schema
    externalParts: Seq[ExternalPartMetadata],
    // derivations schema
    derivations: Array[StructField],
    // hashToSchema is a mapping from a hash string to an associated list of structField, such that if a record was
    // marked by this hash, it means that all the associated structFields have been pre-populated.
    // to generate the hash:
    // - for chronon flattened log table: we use the schema_hash recorded at logging time generated per-row
    // - for regular hive tables: we use the semantic_hash (a snapshot) of the bootstrap part object
    hashToSchema: Map[String, Array[StructField]]
) {

  lazy val fieldNames: Set[String] = fields.map(_.name).toSet

  private lazy val fields: Seq[StructField] = {
    joinParts.flatMap(_.keySchema) ++ joinParts.flatMap(_.valueSchema) ++
      externalParts.flatMap(_.keySchema) ++ externalParts.flatMap(_.valueSchema) ++ derivations
  }
  private lazy val fieldsMap: Map[String, StructField] = fields.map(f => f.name -> f).toMap

  lazy val baseValueNames: Seq[String] = baseValueFields.map(_.name)
  private lazy val baseValueFields: Seq[StructField] = {
    joinParts.flatMap(_.valueSchema) ++ externalParts.flatMap(_.valueSchema)
  }
}

object BootstrapInfo {

  // Build metadata for the join that contains schema information for join parts, external parts and bootstrap parts
  def from(joinConf: api.Join, range: PartitionRange, tableUtils: TableUtils): BootstrapInfo = {

    // Enrich each join part with the expected output schema
    println(s"\nCreating BootstrapInfo for GroupBys for Join ${joinConf.metaData.name}")
    val joinParts: Seq[JoinPartMetadata] = Option(joinConf.joinParts.toScala)
      .getOrElse(Seq.empty)
      .map(part => {
        val gb = GroupBy.from(part.groupBy, range, tableUtils)
        val keySchema = SparkConversions
          .toChrononSchema(gb.keySchema)
          .map(field => StructField(part.rightToLeft(field._1), field._2))
        val valueSchema = gb.outputSchema.fields.map(part.constructJoinPartSchema)
        JoinPartMetadata(part, keySchema, valueSchema, Array.empty) // will be populated below
      })

    // Enrich each external part with the expected output schema
    println(s"\nCreating BootstrapInfo for ExternalParts for Join ${joinConf.metaData.name}")
    val externalParts: Seq[ExternalPartMetadata] = Option(joinConf.onlineExternalParts.toScala)
      .getOrElse(Seq.empty)
      .map(part =>
        ExternalPartMetadata(part, part.keySchemaFull, part.valueSchemaFull, Array.empty)) // will be populated below

    val baseFields = joinParts.flatMap(_.valueSchema) ++ externalParts.flatMap(_.valueSchema)
    val sparkSchema = StructType(SparkConversions.fromChrononSchema(api.StructType("", baseFields.toArray)))
    val baseDf = tableUtils.sparkSession.createDataFrame(
      tableUtils.sparkSession.sparkContext.parallelize(immutable.Seq[Row]()),
      sparkSchema
    )
    val derivedSchema = if (joinConf.isSetDerivations) {
      val projections = joinConf.derivationProjection(baseFields.map(_.name))
      val projectionMap = projections.toMap
      val derivedDf = baseDf.select(
        projections.map {
          case (name, expression) => expr(expression).as(name)
        }.toSeq: _*
      )
      SparkConversions.toChrononSchema(derivedDf.schema).map {
        case (name, dataType) => (StructField(name, dataType), projectionMap(name))
      }
    } else {
      Array.empty[(StructField, String)]
    }

    // For each joinPart and externalPart find out the list of derived fields that depend on it
    def findRequiringFields(inputFields: Array[StructField]): Array[StructField] = {
      if (joinConf.isSetDerivations) {
        derivedSchema
          .filter {
            // Check if the expression contains any fields from the join part by string matching.
            // Note: this will fail if there are two fields from two join parts where one is a prefix of another.
            // it should be fine if they are from the same join parts since the logic here applies at join part level.
            case (_, expression) => inputFields.map(_.name).exists(expression.contains)
          }
          .map(_._1)
      } else {
        inputFields
      }
    }
    val joinPartsEnriched = joinParts.map { m =>
      m.copy(requiringFields = findRequiringFields(m.valueSchema))
    }

    val externalPartsEnriched = externalParts.map { m =>
      m.copy(requiringFields = findRequiringFields(m.valueSchema))
    }

    /*
     * Partition bootstrap into log-based versus regular table-based.
     *
     * Log table requires special handling because unlike regular Hive table, since log table is affected by schema
     * evolution. A NULL column does not simply mean that the correct value was NULL, it could be that for this record,
     * the value was never logged as the column was added more recently. Instead, we must use the schema info encoded
     * in the schema_hash at time of logging to tell whether a column truly was populated or not.
     *
     * For standard Hive tables however, we can simply trust that a NULL column means that its CORRECT value is NULL,
     * that is, we trust all the data that is given to Chronon by its user and it takes precedence over backfill.
     */
    val (logBootstrapParts, tableBootstrapParts) =
      Option(joinConf.bootstrapParts.toScala).getOrElse(Seq.empty).partition { part =>
        // treat log table with additional selects as standard table bootstrap
        val hasSelect = part.isSetQuery && part.query.isSetSelects
        val tblProps = tableUtils.getTableProperties(part.table)
        tblProps.isDefined && tblProps.get.contains(Constants.ChrononLogTable) && !hasSelect
      }

    println(s"\nCreating BootstrapInfo for Log Based Bootstraps for Join ${joinConf.metaData.name}")
    // Verify that join keys are valid columns on the log table
    logBootstrapParts
      .foreach(part => {
        // practically there should only be one logBootstrapPart per Join, but nevertheless we will loop here
        val schema = tableUtils.getSchemaFromTable(part.table)
        val missingKeys = part.keys(joinConf).filterNot(schema.fieldNames.contains)
        assert(
          missingKeys.isEmpty,
          s"Log table ${part.table} does not contain some specified keys: ${missingKeys.prettyInline}"
        )
      })

    // Retrieve schema_hash mapping info from Hive table properties
    val logHashes = logBootstrapParts.flatMap { part =>
      LogFlattenerJob
        .readSchemaTableProperties(tableUtils, part.table)
        .mapValues(LoggingSchema.parseLoggingSchema(_).valueFields.fields)
        .toSeq
    }.toMap

    println(s"\nCreating BootstrapInfo for Table Based Bootstraps for Join ${joinConf.metaData.name}")
    // Verify that join keys are valid columns on the bootstrap source table
    val tableHashes = tableBootstrapParts
      .map(part => {
        val range = PartitionRange(part.query.startPartition, part.query.endPartition)
        val bootstrapQuery = range.genScanQuery(part.query, part.table, Map(Constants.PartitionColumn -> null))
        val bootstrapDf = tableUtils.sql(bootstrapQuery)
        val schema = bootstrapDf.schema
        val missingKeys = part.keys(joinConf).filterNot(schema.fieldNames.contains)
        assert(
          missingKeys.isEmpty,
          s"Table ${part.table} does not contain some specified keys: ${missingKeys.prettyInline}"
        )

        assert(
          !bootstrapDf.columns.contains(Constants.SchemaHash),
          s"${Constants.SchemaHash} is a reserved column that should only be used for chronon log tables"
        )

        val valueFields = SparkConversions
          .toChrononSchema(schema)
          .filterNot {
            case (name, _) => part.keys(joinConf).contains(name)
          }
          .map(field => StructField(field._1, field._2))

        part.semanticHash -> (valueFields, part.table, bootstrapQuery)
      })
      .toMap

    val hashToSchema = logHashes ++ tableHashes.mapValues(_._1).toMap
    val bootstrapInfo =
      BootstrapInfo(joinConf, joinPartsEnriched, externalPartsEnriched, derivedSchema.map(_._1), hashToSchema)

    // validate that all selected fields except keys from (non-log) bootstrap tables match with
    // one of defined fields in join parts or external parts
    for (
      (fields, table, query) <- tableHashes.values;
      field <- fields
    ) yield {

      assert(
        bootstrapInfo.fieldsMap.contains(field.name),
        s"""Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} does NOT have this field
           |Bootstrap Query:
           |${query}
           |""".stripMargin
      )
      assert(
        bootstrapInfo.fieldsMap(field.name) == field,
        s"""Table $table has column ${field.name} with ${field.fieldType}, but Join ${joinConf.metaData.name} has the same field with ${bootstrapInfo
          .fieldsMap(field.name)
          .fieldType}
           |Bootstrap Query:
           |${query}
           |""".stripMargin
      )
    }
    def stringify(schema: Array[StructField]): String = {
      if (schema.isEmpty) {
        "<empty>"
      } else {
        SparkConversions.fromChrononSchema(api.StructType("", schema)).pretty
      }
    }

    println(s"\n======= Finalized Bootstrap Info ${joinConf.metaData.name} =======\n")
    joinPartsEnriched.foreach { metadata =>
      println(s"""Bootstrap Info for Join Part `${metadata.joinPart.groupBy.metaData.name}`
           |Key Schema:
           |${stringify(metadata.keySchema)}
           |Value Schema:
           |${stringify(metadata.valueSchema)}
           |Downstream Derived Schema:
           |${stringify(metadata.requiringFields)}
           |""".stripMargin)
    }
    externalPartsEnriched.foreach { metadata =>
      println(s"""Bootstrap Info for External Part `${metadata.externalPart.fullName}`
           |Key Schema:
           |${stringify(metadata.keySchema)}
           |Value Schema:
           |${stringify(metadata.valueSchema)}
           |Downstream Derived Schema:
           |${stringify(metadata.requiringFields)}
           |""".stripMargin)
    }
    if (derivedSchema.nonEmpty) {
      println(s"""Bootstrap Info for Derivations
                 |${stringify(derivedSchema.map(_._1))}
                 |""".stripMargin)
    }
    println(s"""Bootstrap Info for Log Bootstraps
         |Log Hashes: ${logHashes.keys.prettyInline}
         |""".stripMargin)
    tableHashes.foreach {
      case (_, (schema, _, query)) =>
        println(s"""Bootstrap Info for Table Bootstraps
           |Bootstrap Query:
           |\n${query}\n
           |Bootstrap Schema:
           |${stringify(schema)}
           |""".stripMargin)
    }

    println(s"\n======= Finalized Bootstrap Info ${joinConf.metaData.name} END =======\n")

    bootstrapInfo
  }
}