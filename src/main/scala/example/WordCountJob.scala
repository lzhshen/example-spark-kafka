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

import java.io.File
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI

import org.apache.spark.storage.StorageLevel
import org.mkuthan.spark._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration
import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.clients.producer.ProducerRecord

class WordCountJob(config: WordCountJobConfig, source: KafkaDStreamSource) extends SparkStreamingApplication {

  override def sparkConfig: Map[String, String] = config.spark

  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  def start(): Unit = {

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

      import scala.language.implicitConversions
      implicit def map2Properties(map:Map[String,String]):java.util.Properties = {
        val props = new java.util.Properties()
        map foreach { case (key,value) => props.put(key, value)}
        props
      }

      val p = map2Properties(config.sinkKafka)
      countedWords.writeToKafka(
        p,
        s => new ProducerRecord[String, String](config.outputTopic, s.toString())
      )
    }
  }

}


object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = if (args.length == 0) {
      WordCountJobConfig()
    } else {
      /*
      import java.io.InputStreamReader
      val reader = new InputStreamReader(args(0))
      val config = ConfigFactory.parseReader(reader)*/

      //val configFile = SparkFiles.get(args(0))
      //val config = ConfigFactory.load(ConfigFactory.parseFile(new File(configFile)))

      WordCountJobConfig(WordCountJobConfig.loadConf(args(0)))
    }

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

  private val HDFS_IMPL_KEY = "fs.hdfs.impl"
  def loadConf(pathToConf: String): Config = {
    val path = new Path(pathToConf)
    val confFile = File.createTempFile(path.getName, "tmp")
    confFile.deleteOnExit()
    getFileSystemByUri(path.toUri).copyToLocalFile(path, new Path(confFile.getAbsolutePath))

    ConfigFactory.parseFile(confFile)
  }


  def getFileSystemByUri(uri: URI) : FileSystem  = {
    val hdfsConf = new Configuration()
    hdfsConf.set(HDFS_IMPL_KEY, classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    FileSystem.get(uri, hdfsConf)
  }
}
