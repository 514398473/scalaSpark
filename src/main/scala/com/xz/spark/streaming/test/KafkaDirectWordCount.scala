package com.xz.spark.streaming.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.time.Seconds
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder

object KafkaDirectWordCount {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordcount").setMaster("local[1]");
    val streamingContext = new StreamingContext(sparkConf, Durations.seconds(10));
    val params = Map[String, String]("metadata.broker.list" -> "node1:9092,node2:9092,node3:9092");
    val topics = Set[String]("wordcount");
    val linerdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, params, topics);

    val words = linerdd.flatMap(_._2.split(" "));
    val pairs = words.map { word => (word, 1) };
    val wcs = pairs.reduceByKey(_ + _);
    wcs.print();
    streamingContext.start();
    streamingContext.awaitTermination();
    streamingContext.stop();
  }
}