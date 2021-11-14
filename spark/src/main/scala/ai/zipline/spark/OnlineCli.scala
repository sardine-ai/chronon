package ai.zipline.spark

import ai.zipline.api
import ai.zipline.api.Extensions.{GroupByOps, SourceOps}
import ai.zipline.api.ThriftJsonCodec
import ai.zipline.online.{Api, Fetcher, MetadataStore}
import com.google.gson.GsonBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.thrift.TBase
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}

import java.io.File
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader

// CLI for fetching, metadataupload
object OnlineCli {

  // common arguments to all online commands
  trait OnlineSubcommand { s: ScallopConf =>
    val props: Map[String, String] = props[String]('Z')
    val onlineJar: ScallopOption[String] =
      opt[String](required = true, descr = "Path to the jar contain the implementation of Online.Api class")
    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

    lazy val impl: Api = {
      val urls = Array(new File(onlineJar()).toURI.toURL)
      val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
      val cls = cl.loadClass(onlineClass())
      val constructor = cls.getConstructors.apply(0)
      val onlineImpl = constructor.newInstance(props)
      onlineImpl.asInstanceOf[Api]
    }
  }

  object FetcherCli {

    class Args extends Subcommand("fetch") with OnlineSubcommand {
      val keyJson: ScallopOption[String] = opt[String](required = true, descr = "json of the keys to fetch")
      val name: ScallopOption[String] = opt[String](required = true, descr = "name of the join/group-by to fetch")
      val `type`: ScallopOption[String] = choice(Seq("join", "group-by"), descr = "the type of conf to fetch")
    }

    def run(args: Args): Unit = {
      val gson = (new GsonBuilder()).setPrettyPrinting().create()
      val keyMap = gson.fromJson(args.keyJson(), classOf[java.util.Map[String, AnyRef]]).asScala.toMap

      val fetcher = new Fetcher(args.impl.genKvStore)
      val startNs = System.nanoTime
      val requests = Seq(Fetcher.Request(args.name(), keyMap))
      val resultFuture = if (args.`type`() == "join") {
        fetcher.fetchJoin(requests)
      } else {
        fetcher.fetchGroupBys(requests)
      }
      val result = Await.result(resultFuture, 1.minute)
      val awaitTimeMs = (System.nanoTime - startNs) / 1e6d

      // treeMap to produce a sorted result
      val tMap = new java.util.TreeMap[String, AnyRef]()
      result.head.values.foreach { case (k, v) => tMap.put(k, v) }
      println(gson.toJson(tMap))
      println(s"Fetched in: $awaitTimeMs ms")
    }
  }

  object MetadataUploader {
    class Args extends Subcommand("upload-metadata") with OnlineSubcommand {
      val confPath: ScallopOption[String] =
        opt[String](required = true, descr = "Path to the Zipline config file or directory")
    }

    def run(args: Args): Unit = {
      val metadataStore = new MetadataStore(args.impl.genKvStore, "ZIPLINE_METADATA", timeoutMillis = 10000)
      val putRequest = metadataStore.putConf(args.confPath())
      val res = Await.result(putRequest, 1.hour)
      println(
        s"Uploaded Zipline Configs to the KV store, success count = ${res.count(v => v)}, failure count = ${res.count(!_)}")
    }
  }

  object GroupByStreaming {
    def buildSession(appName: String, local: Boolean): SparkSession = {
      val baseBuilder = SparkSession
        .builder()
        .appName(appName)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "ai.zipline.spark.ZiplineKryoRegistrator")
        .config("spark.kryoserializer.buffer.max", "2000m")
        .config("spark.kryo.referenceTracking", "false")

      val builder = if (local) {
        baseBuilder
        // use all threads - or the tests will be slow
          .master("local[*]")
          .config("spark.local.dir", s"/tmp/zipline-spark-streaming")
          .config("spark.kryo.registrationRequired", "true")
      } else {
        baseBuilder
      }
      builder.getOrCreate()
    }

    def dataStream(session: SparkSession, host: String, topic: String): DataFrame = {
      session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", host)
        .option("subscribe", topic)
        .option("enable.auto.commit", "true")
        .load()
        .selectExpr("value")
    }

    class Args extends Subcommand("group-by-streaming") with OnlineSubcommand {
      val confPath: ScallopOption[String] = opt[String](required = true, descr = "path to groupBy conf")
      val kafkaBootstrap: ScallopOption[String] =
        opt[String](required = true, descr = "host:port of a kafka bootstrap server")
      val mockWrites: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                            default = Some(false),
                                                            descr =
                                                              "flag - to ignore writing to the underlying kv store")
      val debug: ScallopOption[Boolean] = opt[Boolean](required = false,
                                                       default = Some(false),
                                                       descr =
                                                         "Prints details of data flowing through the streaming job")
      val local: ScallopOption[Boolean] =
        opt[Boolean](required = false, default = Some(false), descr = "Launches the job locally")
      def parseConf[T <: TBase[_, _]: Manifest: ClassTag]: T =
        ThriftJsonCodec.fromJsonFile[T](confPath(), check = true)
    }

    def run(args: Args): Unit = {
      val groupByConf: api.GroupBy = args.parseConf[api.GroupBy]
      val session: SparkSession = buildSession(groupByConf.metaData.name, args.local())
      val streamingSource = groupByConf.streamingSource
      assert(streamingSource.isDefined, "There is no valid streaming source - with a valid topic, and endDate < today")
      val inputStream: DataFrame = dataStream(session, args.kafkaBootstrap(), streamingSource.get.topic)
      val streamingRunner =
        new streaming.GroupBy(inputStream, session, groupByConf, args.impl, args.debug(), args.mockWrites())
      streamingRunner.run()
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {

    object FetcherCliArgs extends FetcherCli.Args
    addSubcommand(FetcherCliArgs)
    object MetadataUploaderArgs extends MetadataUploader.Args
    addSubcommand(MetadataUploaderArgs)
    object GroupByStreamingArgs extends GroupByStreaming.Args
    addSubcommand(GroupByStreamingArgs)
    requireSubcommand()
    verify()
  }

  def onlineBuilder(userConf: Map[String, String], onlineJar: String, onlineClass: String): Api = {
    val urls = Array(new File(onlineJar).toURI.toURL)
    val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
    val cls = cl.loadClass(onlineClass)
    val constructor = cls.getConstructors.apply(0)
    val onlineImpl = constructor.newInstance(userConf)
    onlineImpl.asInstanceOf[Api]
  }

  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    args.subcommand match {
      case Some(x) =>
        x match {
          case args.FetcherCliArgs       => FetcherCli.run(args.FetcherCliArgs)
          case args.MetadataUploaderArgs => MetadataUploader.run(args.MetadataUploaderArgs)
          case args.GroupByStreamingArgs => GroupByStreaming.run(args.GroupByStreamingArgs)
          case _                         => println(s"Unknown subcommand: ${x}")
        }
      case None => println(s"specify a subcommand please")
    }
    System.exit(0)
  }
}
