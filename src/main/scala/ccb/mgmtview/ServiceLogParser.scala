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
import org.apache.spark.streaming.{Duration, StreamingContext}
import scala.concurrent.duration.FiniteDuration

object ServiceLogParser {
/*
  type WordCount = (String, Int)

  def parse(
                  ssc: StreamingContext,
                  lines: DStream[String],
                  windowDuration: FiniteDuration,
                  slideDuration: FiniteDuration): DStream[WordCount] = {

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

    val sc = ssc.sparkContext

    val windowDurationVar = sc.broadcast(windowDuration)
    val slideDurationVar = sc.broadcast(slideDuration)

    val words = lines
      .transform(splitLine)

    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, windowDurationVar.value, slideDurationVar.value)

    wordCounts
      .transform(skipEmptyWordCounts)
      .transform(sortWordCounts)
  }

  val splitLine = (lines: RDD[String]) => lines.flatMap(line => line.split("[^\\p{L}]"))
  */

}


