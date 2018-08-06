package org.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object OfflineUtil {

  /**
    * getSparkContext getSparkContext
    *
    * @param appName
    * @param masterType
    * @return
    */
  def getSparkContext(appName: String, masterType: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(masterType)
    conf.setAppName(appName)
    new SparkContext(conf)
  }

  /**
    * getSparkSession getSparkSession
    *
    * @param appName
    * @param masterType
    * @return
    */
  def getSparkSession(appName: String, masterType: String): SparkSession = {
    val conf = new SparkConf()
    conf.setMaster(masterType)
    conf.setAppName(appName)
    if (masterType.equals("local")) {
      conf.set("spark.driver.bindAddress", "127.0.0.1")
    }
    return SparkSession.builder().config(conf).getOrCreate()
  }

  /**
    * dropDirectory dropDirectory
    *
    * @param path
    */
  def dropDirectory(path: String): Unit = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    val _path = new Path(path)
    if (fs.exists(_path)) {
      fs.delete(_path, true)
    }
  }

  /**
    * saveDirectory saveDirectory
    *
    * @param rdd
    * @param path
    */
  def saveDirectory(rdd: RDD[_], path: String): Unit = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(hadoopConf)
    val _path = new Path(path)
    if (fs.exists(_path)) {
      fs.delete(_path, true)
    }
    rdd.saveAsTextFile(path)
  }

  /**
    * readFile readFile
    *
    * @param sc
    * @param filePath
    * @return
    */
  def readFile(sc: SparkContext, filePath: String): RDD[String] = {
    return sc.textFile(filePath)
  }

  /**
    * readFile readFile
    *
    * @param ss
    * @param filePath
    * @return
    */
  def readFile(ss: SparkSession, filePath: String): RDD[String] = {
    return ss.sparkContext.textFile(filePath)
  }

}
