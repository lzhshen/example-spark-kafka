// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ccb.mgmtview

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext}
import org.shen.streaming.FilebeatLogRecord
import org.apache.spark.broadcast.Broadcast

object ServiceLogParser {

  val fields: List[String] = List("timestamp", "loglevel", "reqTimeStamp",
    "uuid", "traceId", "txCodeDetails", "compId", "insId", "userId",
    "loginName", "clientInfo", "txCostTime", "dataFrom", "rowCount",
    "errCode", "errMsg")
  val defaultOrgRec = OrgInfoRec("0", "0", "0", "0", "0", "0",
    "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0") // TODO:
  var orgMap: Map[String, OrgInfoRec] = Map()

  def parse(ssc: StreamingContext,
            lines: DStream[String],
            orgMapBC: Broadcast[Map[String, OrgInfoRec]]): DStream[String] = {
    orgMap = orgMapBC.value
    val docs = lines.transform(extractMessageField).transform(map2String)
    docs
  }

  // for UT only
  def setOrgMap(m: Map[String, OrgInfoRec]): Unit = {
    orgMap = m
  }

  val extractMessageField = (lines: RDD[String]) =>
    lines.map(line => {
      val rec = new FilebeatLogRecord(line, fields)
      val m = rec.parse("\\|")
      val orgInfo = orgMap.getOrElse(m("insId"), defaultOrgRec)
      /*
      val logWithOrg = mutable.Map.empty[String, String]
      logWithOrg ++= m.toList
      logWithOrg += ("id" -> orgInfo.id)*/
      // cut off milliseconds part of timestamp if "timestamp" field exist
      val n = collection.mutable.Map(m.toSeq: _*)
      if (n.contains("timestamp")) n("timestamp") = n("timestamp").split(",")(0)
      n += ("InsID" -> orgInfo.InsID)
      n += ("Inst_Chn_ShrtNm" -> orgInfo.Inst_Chn_ShrtNm)
      n += ("Inst_Hier_Code"-> orgInfo.Inst_Hier_Code)
      n += ("Blng_Lvl7_Inst_ID" -> orgInfo.Blng_Lvl7_Inst_ID)
      n += ("Blng_Lvl7_Inst_Nm" -> orgInfo.Blng_Lvl7_Inst_Nm)
      n += ("Blng_Lvl6_Inst_ID" -> orgInfo.Blng_Lvl6_Inst_ID)
      n += ("Blng_Lvl6_Inst_Nm" -> orgInfo.Blng_Lvl6_Inst_Nm)
      n += ("Blng_Lvl5_Inst_ID" -> orgInfo.Blng_Lvl5_Inst_ID)
      n += ("Blng_Lvl5_Inst_Nm" -> orgInfo.Blng_Lvl5_Inst_Nm)
      n += ("Blng_Lvl4_Inst_ID" -> orgInfo.Blng_Lvl4_Inst_ID)
      n += ("Blng_Lvl4_Inst_Nm" -> orgInfo.Blng_Lvl4_Inst_Nm)
      n += ("Blng_Lvl3_Inst_ID" -> orgInfo.Blng_Lvl3_Inst_ID)
      n += ("Blng_Lvl3_Inst_Nm" -> orgInfo.Blng_Lvl3_Inst_Nm)
      n += ("Blng_Lvl2_InsID" -> orgInfo.Blng_Lvl2_InsID)
      n += ("Blng_Lvl2_Inst_Nm" -> orgInfo.Blng_Lvl2_Inst_Nm)
      n += ("Blng_Lv11_InsID" -> orgInfo.Blng_Lv11_InsID)
      n += ("Blng_Lvl1_Inst_Nm" -> orgInfo.Blng_Lvl1_Inst_Nm)
      n.toMap
    })

  val map2String = (maps: RDD[Map[String, String]]) =>
    maps.map(m => {
      val doc = scala.util.parsing.json.JSONObject(m).toString()
      doc
    })

  /*
  def orgInfoToMap(orgInfo: OrgInfoRec): Map[String, Any] = {
    val fieldNames = orgInfo.getClass.getDeclaredFields.map(_.getName)
    val vals = OrgInfoRec.unapply(orgInfo).get.iterator.toSeq
    fieldNames.zip(vals).toMap
  }*/

}

case class OrgInfoRec (InsID: String,
                       Inst_Chn_ShrtNm: String,
                       Inst_Hier_Code: String,
                       Blng_Lvl7_Inst_ID: String,
                       Blng_Lvl7_Inst_Nm: String,
                       Blng_Lvl6_Inst_ID: String,
                       Blng_Lvl6_Inst_Nm: String,
                       Blng_Lvl5_Inst_ID: String,
                       Blng_Lvl5_Inst_Nm: String,
                       Blng_Lvl4_Inst_ID: String,
                       Blng_Lvl4_Inst_Nm: String,
                       Blng_Lvl3_Inst_ID: String,
                       Blng_Lvl3_Inst_Nm: String,
                       Blng_Lvl2_InsID: String,
                       Blng_Lvl2_Inst_Nm: String,
                       Blng_Lv11_InsID: String,
                       Blng_Lvl1_Inst_Nm: String,
                       StDt: String,
                       EdDt: String)


object OrgLogParser {
  def parse(line: String): OrgInfoRec = {
    val m = line.split("\\,")
    val orgInfoRec = new OrgInfoRec(m(0), m(1),m(2),m(3),m(4),m(5),
      m(6),m(7),m(8),m(9),m(10),
      m(11),m(12),m(13),m(14),m(15),m(16),m(17),m(18))
    orgInfoRec
  }
}







