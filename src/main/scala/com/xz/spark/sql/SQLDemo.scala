package com.xz.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val personRdd = sc.textFile("hdfs://study1:9000/people.txt").map(line => {
      val fields = line.split("\t")
      People(fields(0).toLong, fields(1), fields(2))
    })

    import sqlContext.implicits._
    val personDf = personRdd.toDF
    personDf.registerTempTable("People")
    sqlContext.sql("select * from People where userName = 'user02' order by id desc limit 2").show()
    sc.stop()

  }
}

case class People(id: Long, userName: String, password: String)
