package org.shen.streaming

import java.io.File
import java.net.URI

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark._

/**
  * Created by shen on 7/2/17.
  */
object Utils {

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

  import scala.language.implicitConversions
  implicit def map2Properties(map:Map[String,String]):java.util.Properties = {
    val props = new java.util.Properties()
    map foreach { case (key,value) => props.put(key, value)}
    props
  }
/*
  def loadOrgFromCsvFile(file: String, sc: SparkContext, header: Boolean = true) : Map[String: Map[String, String]] = {
    val df = sc.read.format("csv").option("header", "true").load(file)
  }*/
}

object LogHolder extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)
}

