package org.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

object FileUtils {

  /**
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
}
