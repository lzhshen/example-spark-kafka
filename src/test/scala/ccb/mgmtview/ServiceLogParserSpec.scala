package ccb.mgmtview

/**
  * Created by shen on 7/5/17.
  */

import ccb.mgmtview.ServiceLogParser.extractMessageField
import org.apache.spark.rdd.RDD
import org.mkuthan.spark.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually

object ServiceLogParserSpec {
  val inputDoc: String = """{"@timestamp":"2017-06-27T07:07:14.320Z",
    "beat":{"hostname":"W112PCO3UM12","name":"W112PCO3UM12", "version":"5.4.2"},
    "input_type":"log",
    "message":"2017-06-27 14:14:04,557|INFO |1498544044408|516067101|4600954220170627141138|AP121A001@TEST001002|AP121_test|350616100|1|||149|saiku|2|||",
    "offset":138,
    "source":"/home/ap/p12/shen/data/0.1og",
    "type":"log"
  }"""
  val outputDoc: String =
    """{"insId" : "350616100", "timestamp" : "2017-06-27 14:14:04", "rowCount" : "2", "uuid" : "516067101", "errMsg" : "", "clientInfo" : "", "txCodeDetails" : "AP121A001@TEST001002", "errCode" : "", "txCostTime" : "149", "reqTimeStamp" : "1498544044408", "compId" : "AP121_test", "dataFrom" : "saiku", "userId" : "1", "loginName" : "", "traceId" : "4600954220170627141138", "loglevel" : "INFO "}"""
}

class ServiceLogParserSpec extends FlatSpec with GivenWhenThen with Matchers with Eventually with SparkStreamingSpec {

  import ServiceLogParserSpec._

  "Raw json doc from kafka" should "be converted" in {
    val rawDoc = Seq(inputDoc)

    val newDoc = extractMessageField(sc.parallelize(rawDoc)).collect()

    newDoc shouldBe Array(outputDoc)
  }
}
