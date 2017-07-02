package com.xz.spark.streaming.test

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Durations

object HDFSWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(10));

    val lines = streamingContext.textFileStream("hdfs://node1:8020/test/wordcount");

    val wcs = lines.flatMap { _.split(" ") }.map { word => (word, 1) }.reduceByKey(_ + _);
    wcs.print();
    wcs.saveAsTextFiles("hdfs://node1:8020/test/result/", "txt");
    streamingContext.start();
    streamingContext.awaitTermination();
    streamingContext.stop();
  }
}