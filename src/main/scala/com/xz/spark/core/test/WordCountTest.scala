package com.xz.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xuz-d on 2017/7/25.
  */
object WordCountTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val result = sc.textFile("c://新建文本文档.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    println(result.collect.toBuffer)
    sc.stop()
  }
}
