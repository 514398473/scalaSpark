package com.xz.spark.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new StreamingContext(conf, Seconds(5))
    val result = sc.socketTextStream("study1", 8888).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()
    sc.start()
    sc.awaitTermination()
  }
}
