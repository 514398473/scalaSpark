package com.xz.spark.core.test

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xuz-d on 2017/7/25.
  */
object UrlCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("urlcount").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd0 = sc.textFile("c://itcast.log").map(x => {
      val fields = x.split("\t")
      (fields(1), 1)
    }).reduceByKey(_ + _)
    val rdd1 = rdd0.map(x => {
      val host = new URL(x._1).getHost
      (host, x._1, x._2)
    })
    //将所有的类别排序
    val rdd2 = rdd1.groupBy(_._1).mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(3)
    })
    rdd2.saveAsTextFile("c://out")
    //排序其中某个类别
    //    val rdd2 = rdd1.filter(_._1 == "java.itcast.cn").sortBy(_._3, false).take(3)
    //    println(rdd2.toBuffer)
    sc.stop()
  }
}
