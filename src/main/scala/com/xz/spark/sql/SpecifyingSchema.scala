package com.xz.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object SpecifyingSchema {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("SpecifyingSchema").setMaster("local[1]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //从指定的地址创建RDD
    val peopleRDD = sc.textFile("hdfs://study1:9000/people.txt").map(_.split("\t"))
    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", LongType, true),
        StructField("userName", StringType, true),
        StructField("password", StringType, true)
      )
    )
    //将RDD映射到rowRDD
    val rowRDD = peopleRDD.map(p => Row(p(0).toLong, p(1).trim, p(2).trim))
    //将schema信息应用到rowRDD上
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    //注册表
    peopleDataFrame.registerTempTable("t_people")
    //执行SQL
    val df = sqlContext.sql("select * from t_people order by id desc limit 4")
    //将结果以JSON的方式存储到指定位置
    df.write.json("hdfs://study1:9000/out1")
    //停止Spark Context
    sc.stop()
  }
}
