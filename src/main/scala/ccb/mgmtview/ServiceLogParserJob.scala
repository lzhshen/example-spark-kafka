package ccb.mgmtview

import org.apache.spark.storage.StorageLevel
import org.mkuthan.spark._
import com.typesafe.config.{Config, ConfigFactory}
import net.ceedubs.ficus.Ficus._

import scala.concurrent.duration.FiniteDuration
import com.github.benfradet.spark.kafka010.writer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.shen.streaming.Utils

class ServiceLogParserJob(config: ServiceLogParserJobConfig, source: KafkaDStreamSource)
  extends SparkStreamingApplication {

  override def sparkConfig: Map[String, String] = config.spark

  override def streamingBatchDuration: FiniteDuration = config.streamingBatchDuration

  override def streamingCheckpointDir: String = config.streamingCheckpointDir

  override def streamingShutdownMarker: String = config.streamingShutdownMarker

  def start(): Unit = {

    withSparkStreamingContext { (sc: SparkContext, ssc: StreamingContext) =>
      // load org map file
      val orgFile = config.appParams("orgMapFile")
      val orgMap = sc.textFile(orgFile).collect().drop(1).map(OrgLogParser.parse).map(o => (o.InsID, o)).toMap
      val orgMapBC = sc.broadcast(orgMap)

      val input = source.createSource(ssc, config.inputTopic)
      val lines = input.map(_.value())

      val docs = ServiceLogParser.parse(ssc, lines, orgMapBC)

      docs.persist(StorageLevel.MEMORY_ONLY_SER)

      val p = Utils.map2Properties(config.sinkKafka)
      docs.writeToKafka(
        p,
        s => new ProducerRecord[String, String](config.outputTopic, s.toString())
      )
    }
  }
}


object ServiceLogParserJob {

  def main(args: Array[String]): Unit = {
    val config = if (args.length == 0) {
      ServiceLogParserJobConfig()
    } else {
      ServiceLogParserJobConfig(Utils.loadConf(args(0)))
    }

    val streamingJob = new ServiceLogParserJob(config, KafkaDStreamSource(config.sourceKafka))
    streamingJob.start()
  }
}

case class ServiceLogParserJobConfig(
                               inputTopic: String,
                               outputTopic: String,
                               appParams:Map[String, String],
                               spark: Map[String, String],
                               streamingBatchDuration: FiniteDuration,
                               streamingCheckpointDir: String,
                               streamingShutdownMarker: String,
                               sourceKafka: Map[String, String],
                               sinkKafka: Map[String, String])
  extends Serializable

object ServiceLogParserJobConfig {

  def apply(): ServiceLogParserJobConfig = apply(ConfigFactory.load)

  def apply(applicationConfig: Config): ServiceLogParserJobConfig = {

    val config = applicationConfig.getConfig("serviceLogParserJob")

    new ServiceLogParserJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topic"),
      config.as[Map[String, String]]("appParams"),
      config.as[Map[String, String]]("spark"),
      config.as[FiniteDuration]("streaming.batchDuration"),
      config.as[String]("streaming.checkpointDir"),
      config.as[String]("streaming.shutdownMarker"),
      config.as[Map[String, String]]("kafkaSource"),
      config.as[Map[String, String]]("kafkaSink")
    )
  }
}
