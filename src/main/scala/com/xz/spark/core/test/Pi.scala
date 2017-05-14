package com.xz.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Pi {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("pi").setMaster("local[1]");
    val sparkContext = new SparkContext(sparkConf);
    val slices = 10000;
    val n = 1000 * slices;
    val count = sparkContext.parallelize(1 to n, slices).map { i =>
      def random: Double = Math.random();
      val x = random * 2 - 1;
      val y = random * 2 - 1;
      //println(x + "--" + y);
      if (x * x + y * y < 1) 1 else 0;
    }.reduce(_ + _);
    println("Pi is roughly " + 4.0 * count / n);
    sparkContext.stop();
  }
}