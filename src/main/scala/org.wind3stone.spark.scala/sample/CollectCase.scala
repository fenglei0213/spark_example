package org.wind3stone.spark.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object CollectCase {

  def main(args: Array[String]): Unit = {
    //
    val conf = new SparkConf().setAppName("Collect Application").setMaster("local");
    val sc = new SparkContext(conf)
    val outputRdd = sc.parallelize(List(1, 2, 3, 3));
    // outputRdd.collect 加括号和不加括号的区别是?
    // rdd 执行 collect 方法后，不返回 rdd 如何打印?原rdd没有变化
    outputRdd.collect().foreach(println);
  }
}
