package org.shen.streaming

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.streaming.StreamingContext

/**
  * Created by shen on 7/2/17.
  */
object Utils {
  val log = Logger.getLogger(getClass)
/*
  def waitShutdownCommand(ssc: StreamingContext, fs: FileSystem, controlFilePath: String): Unit = {
    var stopFlag: Boolean = false
    val checkIntervalMillis = 10000
    var isStopped = false
    while (!isStopped) {
      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
      if (isStopped)
        log.info("ssc stopped")

      if (!stopFlag) {
        stopFlag = fs.exists(new Path(controlFilePath))
      } else {
        log.info("waiting ssc to stop")
      }

      if (!isStopped && stopFlag) {
        log.info("stopping ssc")
        ssc.stop(true, true)
        log.info("ssc stop called")
      }
    }
  }
  */
}
