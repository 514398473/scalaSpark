package com.xz.spark.core.test

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by xuz-d on 2017/7/25.
  */
object UrlCountPartition {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlcount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd0 = sc.textFile("c://itcast.log").map(x => {
      val fields = x.split("\t")
      (fields(1), 1)
    }).reduceByKey(_ + _)
    val rdd1 = rdd0.map(x => {
      val host = new URL(x._1).getHost
      (host, (x._1, x._2))
    })
    val rdd2 = rdd1.map(_._1).distinct().collect()
    //    val rdd3 = rdd1.partitionBy(new HashPartitioner(rdd2.length)).mapPartitions(it => {
    //      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    //    })
    val rdd3 = rdd1.partitionBy(new MyPartition(rdd2)).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })
    rdd3.saveAsTextFile("c://out")
    //    println(rdd2.toBuffer)
    sc.stop()
  }
}

class MyPartition(institutes: Array[String]) extends Partitioner {

  val map = new mutable.HashMap[String, Int]()
  var count = 0
  for (i <- institutes) {
    map += (i -> count)
    count += 1
  }

  override def numPartitions: Int = institutes.length

  override def getPartition(key: Any): Int = {
    map.getOrElse(key.toString, 0)
  }
}