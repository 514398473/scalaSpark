package com.xz.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PageRank {

  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf().setAppName("pagerank").setMaster("local[1]");
    val sparkContext = new SparkContext(sparkconf);
    val iters = 20;
    val lines = sparkContext.textFile("page.txt");
    val links = lines.map { s =>
      val parts = s.split("\\s+");
      (parts(0), parts(1));
    }.distinct().groupByKey().cache;
    links.foreach(println);
    var ranks = links.mapValues { link => 1.0 };
    ranks.foreach(println);
    for (i <- 1 to iters) { //(1,5),1.0
      val contribs = links.join(ranks).values.flatMap {
        case (urls, rank) =>
          val size = urls.size
          urls.map { url => (url, rank / size) }
      }
      ranks = contribs.reduceByKey(_+_).mapValues {0.15 + 0.85 * _ }
    }
    val result = ranks.collect();
    result.foreach(i => println( i._1 + ":" + i._2));
    sparkContext.stop();
  }
}