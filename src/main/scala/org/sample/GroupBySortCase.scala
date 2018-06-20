package org.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.utils.FileUtils

object GroupBySortCase {

  def main(args: Array[String]): Unit = {
    //
    val conf = new SparkConf().setAppName("Collect Application").setMaster("local");
    val sc = new SparkContext(conf)
    val aFilePath = "/code/github_my/spark_example/data/group/in"
    val outFilePath = "/code/github_my/spark_example/data/group/out"
    val aDataRdd = sc.textFile(aFilePath, 1)
    val rsDataRdd = aDataRdd.map(
      line => {
        val lineArray = line.split(",")
        (lineArray(0), lineArray(1).toInt)
      }
    ).groupByKey(
    ).map(item => {
      // 只排第一行 (19,List(117, 140, 159))
      //(18,List(120, 126, 90))
      // (item._1, item._2.toList.sortWith((a, b) => a > b).take(1))
      // (item._1, item._2.toList.sorted)
      (item._1, item._2.toList.sortWith((a, b) => a > b).take(1))
    })
    //.map(item => {
    //   (item._2)
    // (item._2.toList.sortBy(item => (item._1)).take(1))
    //})
    // 不写点语法也对
    FileUtils.dropDirectory(outFilePath);
    rsDataRdd.saveAsTextFile(outFilePath);
  }
}
