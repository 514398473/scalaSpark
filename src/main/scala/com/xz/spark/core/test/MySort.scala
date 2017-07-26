package com.xz.spark.core.test

import org.apache.spark.{SparkConf, SparkContext}

object OrderContext {
  implicit def girlOrdered = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue) {
        if (x.age > y.age) -1 else 1
      } else -1
    }
  }
}

object MySort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("mysort")
    val sc = new SparkContext(conf)
    val rdd0 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2), ("JuJingYi", 95, 22, 3)))
    import OrderContext.girlOrdered
    val rdd1 = rdd0.sortBy(x => Girl(x._2, x._3), false)
    println(rdd1.collect().toBuffer)
    sc.stop()
  }
}

/*
case class Girl(faceValue: Int, age: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if (this.faceValue == that.faceValue) {
      that.age - this.age
    } else {
      this.faceValue - that.faceValue
    }
  }
}*/

case class Girl(faceValue: Int, age: Int) extends Serializable