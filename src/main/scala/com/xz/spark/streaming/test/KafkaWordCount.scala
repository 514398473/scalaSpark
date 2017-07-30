package com.xz.spark.streaming.test

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by root on 2016/5/21.
  */
object KafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    //iter.map{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iter.map{ case(word, current_count, history_count) => (word, current_count.sum + history_count.getOrElse(0)) }
  }


  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("c://ck2")
    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    //"Array((alog-2016-04-16, 2), (alog-2016-04-17, 2), (alog-2016-04-18, 2))"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
