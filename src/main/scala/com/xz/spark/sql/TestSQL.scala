package com.xz.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object TestSQL {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestSQL").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val ssc = new SQLContext(sc)
    val lineRDD = sc.textFile("c://person.txt").map(_.split(" "))
    val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    import ssc.implicits._
    val personDF = personRDD.toDF()
    personDF.registerTempTable("t_person")
    ssc.sql("select * from t_person").write.json("c://t_person.txt")
    sc.stop()
  }
}
