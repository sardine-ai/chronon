package ai.chronon.online

import ai.chronon.api.Extensions._
import ai.chronon.api._
import com.timgroup.statsd.NonBlockingStatsDClient

object Metrics {
  object Environment extends Enumeration {
    type Environment = String
    val MetaDataFetching = "metadata.fetch"
    val JoinFetching = "join.fetch"
    val GroupByFetching = "group_by.fetch"
    val GroupByUpload = "group_by.upload"
    val GroupByStreaming = "group_by.streaming"

    val JoinOffline = "join.offline"
    val GroupByOffline = "group_by.offline"
    val StagingQueryOffline = "staging_query.offline"
  }
  import Environment._

  object Tag {
    val GroupBy = "group_by"
    val Join = "join"
    val JoinPartPrefix = "join_part_prefix"
    val StagingQuery = "staging_query"
    val Environment = "environment"
    val Production = "production"
    val Accuracy = "accuracy"
    val Team = "team"
  }

  object Name {
    val FreshnessMillis = "freshness.millis"
    val FreshnessMinutes = "freshness.minutes"
    val LatencyMillis = "latency.millis"
    val LatencyMinutes = "latency.minutes"

    val PartitionCount = "partition.count"
    val RowCount = "row.count"
    val RequestCount = "request.count"

    val Bytes = "bytes"
    val KeyBytes = "key.bytes"
    val ValueBytes = "value.bytes"

    val Exception = "exception"
  }

  object Context {
    val sampleRate: Double = 0.1

    def apply(environment: Environment, join: Join): Context = {
      Context(
        environment = environment,
        join = join.metaData.cleanName,
        production = join.metaData.isProduction,
        team = join.metaData.team
      )
    }

    def apply(environment: Environment, groupBy: GroupBy): Context = {
      Context(
        environment = environment,
        groupBy = groupBy.metaData.cleanName,
        production = groupBy.metaData.isProduction,
        accuracy = groupBy.inferredAccuracy,
        team = groupBy.metaData.team
      )
    }

    def apply(joinContext: Context, joinPart: JoinPart): Context = {
      joinContext.copy(groupBy = joinPart.groupBy.metaData.cleanName,
                       accuracy = joinPart.groupBy.inferredAccuracy,
                       joinPartPrefix = joinPart.prefix)
    }

    def apply(environment: Environment, stagingQuery: StagingQuery): Context = {
      Context(
        environment = environment,
        groupBy = stagingQuery.metaData.cleanName,
        production = stagingQuery.metaData.isProduction,
        team = stagingQuery.metaData.team
      )
    }

    val statsCache: TTLCache[Context, NonBlockingStatsDClient] = new TTLCache[Context, NonBlockingStatsDClient](
      { ctx =>
        println(s"Building new stats cache for ${ctx.toString}".stripMargin)
        assert(ctx.environment != null && ctx.environment.nonEmpty, "Please specify a proper context")
        new NonBlockingStatsDClient("ai.zipline." + ctx.environment + Option(ctx.suffix).map("." + _).getOrElse(""),
                                    "localhost",
                                    8125,
                                    ctx.toTags: _*)
      }
    )
  }

  case class Context(environment: Environment,
                     join: String = null,
                     groupBy: String = null,
                     stagingQuery: String = null,
                     production: Boolean = false,
                     accuracy: Accuracy = null,
                     team: String = null,
                     joinPartPrefix: String = null,
                     mode: String = null,
                     suffix: String = null)
      extends Serializable {

    def withSuffix(suffixN: String): Context = copy(suffix = (Option(suffix) ++ Seq(suffixN)).mkString("."))
    // Tagging happens to be the most expensive part(~40%) of reporting stats.
    // And reporting stats is about 30% of overall fetching latency.
    // So we do array packing directly instead of regular string interpolation.
    // This simply creates "key:value"
    // The optimization shaves about 2ms of 6ms of e2e overhead for 500 batch size.
    def buildTag(key: String, value: String): String = {
      val charBuf = new Array[Char](key.length + value.length + 1)
      key.getChars(0, key.length, charBuf, 0)
      value.getChars(0, value.length, charBuf, key.length + 1)
      charBuf.update(key.length, ':')
      new String(charBuf)
    }

    @transient private lazy val stats: NonBlockingStatsDClient = Metrics.Context.statsCache(this)

    def increment(metric: String): Unit = stats.increment(metric)
    def incrementException(exception: Throwable): Unit = {
      val stackRoot = exception.getStackTrace.apply(0)
      val file = stackRoot.getFileName
      val line = stackRoot.getLineNumber
      val method = stackRoot.getMethodName
      val exceptionSignature = s"[$method@$file:$line]${exception.getClass.toString}"
      stats.increment(Name.Exception, s"${Metrics.Name.Exception}:${exceptionSignature}")
    }

    def histogram(metric: String, value: Double): Unit = stats.histogram(metric, value, Context.sampleRate)
    def histogram(metric: String, value: Long): Unit = stats.histogram(metric, value, Context.sampleRate)
    def count(metric: String, value: Long): Unit = stats.count(metric, value)
    def gauge(metric: String, value: Long): Unit = stats.gauge(metric, value)

    private[Metrics] def toTags: Array[String] = {
      assert(join != null || groupBy != null, "Either Join, groupBy should be set.")
      assert(
        environment != null,
        "Environment needs to be set - group_by.upload, group_by.streaming, join.fetching, group_by.fetching, group_by.offline etc")
      val buffer = new Array[String](8)
      var counter = 0
      def addTag(key: String, value: String): Unit = {
        if (value == null) return
        assert(counter < buffer.length, "array overflow")
        buffer.update(counter, buildTag(key, value))
        counter += 1
      }

      addTag(Tag.Join, join)
      addTag(Tag.GroupBy, groupBy)
      addTag(Tag.StagingQuery, stagingQuery)
      addTag(Tag.Production, production.toString)
      addTag(Tag.Team, team)
      addTag(Tag.Environment, environment)
      addTag(Tag.JoinPartPrefix, joinPartPrefix)
      addTag(Tag.Accuracy, if (accuracy != null) accuracy.name() else null)
      buffer
    }
  }
}