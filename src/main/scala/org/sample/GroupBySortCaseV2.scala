package org.sample

import org.apache.spark.{SparkConf, SparkContext}
import org.utils.OfflineUtil

object GroupBySortCaseV2 {

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
        (lineArray(0).toInt, lineArray(1).toInt)
      }
    ).groupBy(item => (item._1)).map(line=>{
            (line._1,line._2.toList.sortBy(_._2)(Ordering.Int.reverse).take(1))
    })
    OfflineUtil.dropDirectory(outFilePath);
    rsDataRdd.saveAsTextFile(outFilePath);
  }
}
