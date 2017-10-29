package org.wind3stone.spark.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object JoinCase {

  def main(args: Array[String]) {
    val aFilePath = "file:///code/github/spark_example/data/a.txt" // Should be some file on your system
    val bFilePath = "file:///code/github/spark_example/data/a.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Join Application").setMaster("local");
    val sc = new SparkContext(conf)
    val aDataRdd = sc.textFile(aFilePath, 2).cache()
    val bDataRdd = sc.textFile(bFilePath, 2).cache()
    println(bDataRdd.toString())

//    val joinRdd = aDataRdd join bDataRdd
    aDataRdd.foreach(f => println(f))
//    aDataRdd.foreach(f => if(bDataRdd.))

//    val rdd0 = sc.parallelize((1,1),(1,2),(1,3),(2,1),(2,2),(2,3), 3)
//    val rdd5 = rdd0.join(rdd0)
//    aDataRdd.co
//    aDataRdd.collect
    val output = "C:/code/github/spark_example/data/wordcount-" + System.currentTimeMillis();
//    joinRdd.saveAsTextFile(output) //save result to local file or HDFS
  }
}
