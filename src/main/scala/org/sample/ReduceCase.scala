package org.wind3stone.spark.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object ReduceCase {
  def main(args: Array[String]): Unit = {
    //
    val conf = new SparkConf().setAppName("Reduce Application").setMaster("local");
    val sc = new SparkContext(conf)
//    val c = sc.parallelize(1 to 10)
//    val d = c.reduce((x, y) => x + y) //结果55
//    c.foreach(println)
//    println(d);

    val inputRdd = sc.parallelize(List((1,2),(1,3),(3,4),(3,6)))
    val outputRdd = inputRdd.reduceByKey((x,y) => x + y)
    outputRdd.foreach(println)

  }
}
