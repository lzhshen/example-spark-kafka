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

object ServiceLogParser {

  val fields: List[String] = List("timestamp", "loglevel", "reqTimeStamp",
    "uuid", "traceId", "txCodeDetails", "compId", "insId", "userId",
    "loginName", "clientInfo", "txCostTime", "dataFrom", "rowCount",
    "errCode", "errMsg")

  def parse(ssc: StreamingContext, lines: DStream[String]): DStream[String] = {
    val sc = ssc.sparkContext
    val docs = lines.transform(extractMessageField)
    docs
  }

  val extractMessageField = (lines: RDD[String]) =>
    lines.map(line => {
      val rec = new FilebeatLogRecord(line, fields)
      val m = rec.parse("\\|")
      // cut off milliseconds part of timestamp if "timestamp" field exists
      val n = collection.mutable.Map(m.toSeq: _*)
      if (n.contains("timestamp")) n("timestamp") = n("timestamp").split(",")(0)
      val doc = scala.util.parsing.json.JSONObject(n.toMap).toString()
      doc
    })
}


