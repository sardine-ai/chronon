package ai.chronon.quickstart.online

import ai.chronon.online.Fetcher.Request

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object Main {
  def main(args: Array[String]): Unit = {
    val myParams: Map[String, String] = Map("project_id" -> "indigo-computer-272415", "instance_id" -> "data-features", "log_table" -> "chronon.logs")
    val api = new ChrononBigtableOnlineImpl(myParams)
    api.setTimeout(10000000)

    // Build fetcher
    val fetcher = api.buildFetcher(debug=true)

    // Fetch values
//    fetcher.fetchGroupBys(Seq(Request(name = "crn/txns.gby_payment_method", keys = Map("payment_method_type" -> "bank"))))
    val req = Request(name = "crn.txns.gby_payment_method", keys = Map("payment_method_type" -> "card"), atMillis = Some(1727186000000L))
    val responseFuture = fetcher.fetchGroupBys(Seq(req))
//
//    val response = Await.result(responseFuture, 10.seconds)

    responseFuture.onComplete({
      case Success(listInt) => {
        println(listInt)
        listInt.map(response => println(response.values))
      }
      case Failure(exception) => {
        println(exception.getMessage)
      }
    })
    // Set partition specification if needed
    // fetcher.setPartitionSpec("yyyyMMdd")
  }
}