package com.xz.spark.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object WordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
    val sparkContext = new StreamingContext(sparkConf, Seconds(10));

    val lines = sparkContext.socketTextStream("node1", 9999);

    val words = lines.flatMap { _.split(" "); };
    val pairs = words.map { word => (word, 1); };
    val wcs = pairs.reduceByKey(_ + _);
    wcs.print()
    sparkContext.start();
    sparkContext.awaitTermination();

  }

}