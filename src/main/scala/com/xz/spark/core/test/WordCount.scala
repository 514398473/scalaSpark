package com.xz.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
    val sparkContext = new SparkContext(conf);

    val lines = sparkContext.textFile("file:\\C:\\Users\\Administrator\\Desktop\\LICENSE.txt", 1);
    val words = lines.flatMap { line => line.split(" ") };
    val pairs = words.map { word => (word, 1) };
    val wcs = pairs.reduceByKey(_ + _);
    val tempwcs = wcs.map(wc => (wc._2, wc._1));
    val sortwcs = tempwcs.sortByKey(false, 1);
    val resultwcs = sortwcs.map(wc => (wc._2, wc._1));
    val result = resultwcs.filter(wc => wc._1.length() == 5);
    result.foreach(x => println(x._1 + " appears " + x._2 + " times."));
    sparkContext.stop();
  }

}