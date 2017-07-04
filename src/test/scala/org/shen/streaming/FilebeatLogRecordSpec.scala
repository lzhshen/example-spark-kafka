package org.shen.streaming

/**
  * Created by shen on 7/3/17.
  */
import org.scalatest._

object FilebeatLogRecordSpec {
  val doc: String = """{"@timestamp":"2017-06-27T07:07:14.320Z",
    "beat":{"hostname":"W112PCO3UM12","name":"W112PCO3UM12", "version":"5.4.2"},
    "input_type":"log",
    "message":"2017-06-27 14:14:04,557|INFO |1498544044408|516067101|4600954220170627141138|AP121A001@TEST001002|AP121_test|350616100|1|||149|saiku|2|||",
    "offset":138,
    "source":"/home/ap/p12/shen/data/0.1og",
    "type":"log"
  }"""
  val fields: List[String] = List("timestamp", "loglevel", "reqTimeStamp",
      "uuid", "traceId", "txCodeDetails", "compId", "insId", "userId",
      "loginName", "clientInfo", "txCostTime", "dataFrom", "rowCount",
      "errCode", "errMsg")
}

class FilebeatLogRecordSpec extends FlatSpec with GivenWhenThen with Matchers {

  "Line" should "be split into words" in {
    val rec = new FilebeatLogRecord(FilebeatLogRecordSpec.doc, FilebeatLogRecordSpec.fields)
    val m = rec.parse("\\|")

    m shouldBe Map(
      "timestamp" -> "2017-06-27 14:14:04,557",
      "loglevel" -> "INFO ",
      "reqTimeStamp" -> "1498544044408",
      "uuid" -> "516067101",
      "traceId" -> "4600954220170627141138",
      "txCodeDetails" -> "AP121A001@TEST001002",
      "compId" -> "AP121_test",
      "insId" -> "350616100",
      "userId" -> "1",
      "loginName" -> "",
      "clientInfo" -> "",
      "txCostTime" -> "149",
      "dataFrom" -> "saiku",
      "rowCount" -> "2",
      "errCode" -> "",
      "errMsg" -> "")
  }
}
