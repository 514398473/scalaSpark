package com.xz.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by xuz-d on 2017/7/25.
  */
object UserLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("userlocation").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val rdd0 = sc.textFile("c://userlocal").map(x => {
      val fields = x.split(",")
      val phone = fields(0)
      val time = fields(1)
      val lac = fields(2)
      val flag = fields(3)
      ((phone, lac), if (flag == "1") -time.toLong else time.toLong)
    })
    val rdd1 = rdd0.reduceByKey(_ + _)
    val rdd2 = rdd1.map(x => {
      (x._1._2, (x._1._1, x._2))
    })
    val rdd3 = sc.textFile("c://loc_info.txt").map(x => {
      val fields = x.split(",")
      (fields(0), (fields(1), fields(2)))
    })
    val rdd4 = rdd2.join(rdd3).map(x => {
      (x._2._1._1, x._1, x._2._1._2, x._2._2._1, x._2._2._2)
    })
    val rdd5 = rdd4.groupBy(_._1)
    val rdd6 = rdd5.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)
    })
    //println(rdd6.collect().toBuffer)
    rdd6.saveAsTextFile("c://out")
    sc.stop()
  }
}
