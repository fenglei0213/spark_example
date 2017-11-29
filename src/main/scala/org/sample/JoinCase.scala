package org.wind3stone.spark.sample;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object JoinCase {

  def main(args: Array[String]) {
    val aFilePath = "file:///code/github/spark_example/data/a.txt" // Should be some file on your system
    val bFilePath = "file:///code/github/spark_example/data/a.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Join Application").setMaster("local");
    //
    val conf = new SparkConf().setAppName("Join Application").setMaster("local");
    val sc = new SparkContext(conf)
//    val cDataRdd = aDataRdd.map(row=>row.split(";")).map(row=>{(row._1,(row._1,row._2))}).leftOuterJoin(bDataRdd)
//    println(cDataRdd.foreach(println))

    var rdd1 = sc.makeRDD(Array(("A","1"),("B","2"),("C","3")),2)
    var rdd2 = sc.makeRDD(Array(("A","a"),("C","c"),("D","d")),2)
    rdd1.leftOuterJoin(rdd2).collect


    //
    val aDataRdd = sc.textFile(aFilePath, 2).cache()
    val aDataRddNew = aDataRdd.map(line => {
      val lineArray = line.split(";")
      (lineArray(0), lineArray(1))
    })
    val bDataRdd = sc.textFile(bFilePath, 2).cache()
    val bDataRddNew = bDataRdd.map(line => {
      var lineArray = line.split(";")
      (lineArray(0), lineArray(1))
    })
    val cDataRddNew = aDataRddNew.leftOuterJoin(bDataRddNew);
    //    cDataRddNew.foreach(line => {
    //      println(line)
    //    });
    println(cDataRddNew.count())
    val outputFile = "file:///code/github/spark_example/data/output_rs"
    cDataRddNew.saveAsTextFile(outputFile)

//    val joinRdd = aDataRdd join bDataRdd
//    aDataRdd.foreach(f => println(f))
//    aDataRdd.foreach(f => if(bDataRdd.))

//    val rdd0 = sc.parallelize((1,1),(1,2),(1,3),(2,1),(2,2),(2,3), 3)
//    val rdd5 = rdd0.join(rdd0)

//    aDataRdd.collect
//    val output = "C:/code/github/spark_example/data/wordcount-" + System.currentTimeMillis();
//    joinRdd.saveAsTextFile(output) //save result to local file or HDFS
  }
}
