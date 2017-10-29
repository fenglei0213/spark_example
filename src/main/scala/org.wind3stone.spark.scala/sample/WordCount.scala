package org.wind3stone.spark.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object WordCount {
  def main(args: Array[String]) {
    val logFilePath = "file:///code/github/spark_example/data/sample.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Word Application").setMaster("local");
    val sc = new SparkContext(conf)
    val logDataRdd = sc.textFile(logFilePath, 1).cache()

    /**
      * 最后一个参数控制1或2,执行println打印出的数据组不同
      *
      **/
    logDataRdd.foreach(println)
    logDataRdd.foreach(f => print(f))
    // 输出到本地文件系统
    val outputFile = "file:///code/github/spark_example/data/output_rs"
    logDataRdd.saveAsTextFile(outputFile)

    // 集合简单过滤
    val numAs = logDataRdd.filter(line => line.contains("a")).count()
    val numBs = logDataRdd.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))


  }
}
