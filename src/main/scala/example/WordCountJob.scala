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

package example

import org.apache.spark.storage.StorageLevel
import org.mkuthan.spark._

import scala.concurrent.duration.FiniteDuration
import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

class WordCountJob(config: WordCountJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {

  override def sparkConfig: Map[String, String] = config.spark

  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  def start(): Unit = {

    //implicit def map2Properties(map:Map[String,String]):java.util.Properties = {
    //  (new java.util.Properties /: map) {case (props, (k,v)) => props.put(k,v); props}
    //}
    import scala.language.implicitConversions
    implicit def map2Properties(map:Map[String,String]):java.util.Properties = {
      val props = new java.util.Properties()
      map foreach { case (key,value) => props.put(key, value)}
      props
    }

    val p = map2Properties(config.sinkKafka)
    p.setProperty("key.serializer", classOf[StringSerializer].getName)
    p.setProperty("value.serializer", classOf[StringSerializer].getName)

    withSparkStreamingContext { (sc, ssc) =>
      val input = source.createSource(ssc, config.inputTopic)
      val lines = input.map(_.value())

      val countedWords = WordCount.countWords(
        ssc,
        lines,
        config.stopWords,
        config.windowDuration,
        config.slideDuration
      )

      countedWords.persist(StorageLevel.MEMORY_ONLY_SER)

      countedWords.writeToKafka(
        p,
        s => new ProducerRecord[String, String](config.outputTopic, s.toString())
      )
    }
  }

}



object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = WordCountJobConfig()

    val streamingJob = new WordCountJob(config, KafkaDStreamSource(config.sourceKafka))
    streamingJob.start()
  }

}

case class WordCountJobConfig(
                               inputTopic: String,
                               outputTopic: String,
                               stopWords: Set[String],
                               windowDuration: FiniteDuration,
                               slideDuration: FiniteDuration,
                               spark: Map[String, String],
                               streamingBatchDuration: FiniteDuration,
                               streamingCheckpointDir: String,
                               sourceKafka: Map[String, String],
                               sinkKafka: Map[String, String])
  extends Serializable

object WordCountJobConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._

  def apply(): WordCountJobConfig = apply(ConfigFactory.load)

  def apply(applicationConfig: Config): WordCountJobConfig = {

    val config = applicationConfig.getConfig("wordCountJob")

    new WordCountJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topic"),
      config.as[Set[String]]("stopWords"),
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[Map[String, String]]("spark"),
      config.as[FiniteDuration]("streamingBatchDuration"),
      config.as[String]("streamingCheckpointDir"),
      config.as[Map[String, String]]("kafkaSource"),
      config.as[Map[String, String]]("kafkaSink")
    )
  }
}
