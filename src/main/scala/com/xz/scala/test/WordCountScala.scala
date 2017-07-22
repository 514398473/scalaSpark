package com.xz.scala.test

object WordCountScala {

  def main(args: Array[String]): Unit = {
    val lines = List("hello tom hello jerry", "hello jerry", "hello kitty")
    var result = lines.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.size)
    println(result)
  }

}
