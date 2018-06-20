package org.wind3stone.spark.sample


object ListCase {

  def main(args: Array[String]): Unit = {
    var a = 0;
    val numList = List(1, 2, 3, 4, 5, 6);

    // for 循环
    for (a <- numList) {
      println("Value of a: " + a);
    }
  }
}