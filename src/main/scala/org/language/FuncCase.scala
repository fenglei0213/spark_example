package org.wind3stone.spark.scala.language

object FuncCase {

  def main(args: Array[String]): Unit = {
    //
    //    val sum = this.addInt(1, 2)
    //    println(sum)
    //    this.printMe()

    // 匿名函数
    // http://www.yiibai.com/scala/anonymous_functions.html
    //    var mul = (x: Int, y: Int) => x + y
    //    println(mul(1, 2));

    // 复杂匿名函数
    var mul = (x: Int, y: Int) => if (x == 1) {
      x + y
    } else {
      x * y
    }
    println(mul(2, 10))
  }

  def addInt(a: Int, b: Int): Int = {
    var sum: Int = 0
    sum = a + b
    return sum
  }

  def printMe(): Unit = {
    println("Hello, Scala!")
  }
}
