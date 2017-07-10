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

package org.mkuthan.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.shen.streaming._

import scala.concurrent.duration.FiniteDuration

trait SparkStreamingApplication extends SparkApplication with Serializable {

  def streamingBatchDuration: FiniteDuration

  def streamingCheckpointDir: String

  def streamingShutdownMarker: String

  def withSparkStreamingContext(f: (SparkContext, StreamingContext) => Unit): Unit = {
    withSparkContext { sc =>
      val ssc = new StreamingContext(sc, Seconds(streamingBatchDuration.toSeconds))
      ssc.checkpoint(streamingCheckpointDir)

      f(sc, ssc)

      ssc.start()
      waitShutdownCommand(ssc, streamingShutdownMarker)
    }
  }
  //val log = Logger.getLogger(getClass)

  def waitShutdownCommand(ssc: StreamingContext, shutdownMarker: String): Unit = {
    var stopFlag: Boolean = false
    val checkIntervalMillis: Long = 10000
    var isStopped = false
    val fs = FileSystem.get(new Configuration())
    while (!isStopped) {
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped) {
        LogHolder.log.info("ssc stopped")
      }

      if (!stopFlag) {
        stopFlag = fs.exists(new Path(shutdownMarker))
      } else {
        LogHolder.log.info("waiting ssc to stop")
      }

      if (!isStopped && stopFlag) {
        LogHolder.log.info("stopping ssc")
        ssc.stop(true, true)
        LogHolder.log.info("ssc stop called")
      }
    }
  }
}
