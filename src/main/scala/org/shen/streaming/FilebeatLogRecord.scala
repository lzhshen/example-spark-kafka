package org.shen.streaming

import scala.util.parsing.json._
/**
  * Created by shen on 7/2/17.
  */

/** A class to extract log in json format comming from filebeat (via kafka)
  *
  *  @constructor create a new record with a doc and fields list
  *  @param doc the log in json format, like following:
  *             {"@timestamp":"2017-06-27T07:07:14.320Z",
  *              "beat":{"hostname":"W112PCO3UM12","name":"W112PCO3UM12", "version":"5.4.2"},
  *              "input_type":"log",
  *              "message":"2017-06-27 14:14:04,557|INFO |1498544044408|516067101|4600954220170627141138|AP121A001@TEST001002|AP121_test|350616100|1||149|saiku|2|||",
  *              "offset":138,
  *              "source":"/home/ap/p12/shen/data/0.1og",
  *              "type":"log"
}
  *  @param fields the field name list
  */
class FilebeatLogRecord(doc: String, fields: List[String]) {
  def parse(separatorStr: String): Map[String, String] = {
    val result = JSON.parseFull(doc)
    val msg = result match {
      case Some(map) => {
        val m = map.asInstanceOf[Map[String, String]]
        m("message")
      }
      case None => ""
    }
    val values = msg.split(separatorStr, -1)
    val kvMap: Map[String, String] = (fields zip values).toMap
    kvMap
  }
}
/*
case class ServiceLogRec(timeStamp: String,
                         logLevel: String,
                         reqTimeStamp: String,
                         uuid: String,
                         traceID: String,
                         txCodeDetails: String,
                         compId: String,
                         insId: String,
                         userId: String,
                         loginName: String,
                         clientInfo: String,
                         txCostTime: String,
                         dataFrom: String,
                         rowCount: String,
                         errCode: String,
                         errMsg: String)*/
