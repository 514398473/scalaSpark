package com.xz.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object InferringSchema {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("InferringSchema").setMaster("local[1]")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //从指定的地址创建RDD
    val lineRDD = sc.textFile("hdfs://study1:9000/person.txt").map(_.split(" "))

    //创建case class
    //将RDD和case class关联
    val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    //导入隐式转换，如果不到人无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    //注册表
    personDF.registerTempTable("t_person")
    //传入SQL
    val df = sqlContext.sql("select * from t_person order by age desc limit 2")
    //将结果以JSON的方式存储到指定位置
    df.write.json("hdfs://study1:9000/out2")
    //停止Spark Context
    sc.stop()

  }
}

//case class一定要放到外面
case class Person(id: Int, name: String, age: Int)
