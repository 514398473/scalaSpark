package com.xz.scala.test

object TestScala {

  def main(args: Array[String]): Unit = {
    //创建一个List
    val list = List(1, 7, 9, 8, 0, 3, 5, 4, 6, 2)
    //将list中的偶数取出来生成一个新的集合
    val list1 = list.filter(_ % 2 == 0)
    //将list中每个元素乘以10后生成一个新的集合
    val list2 = list.map(_ * 10)
    //将list排序后生成一个新的集合
    //list.sortWith(_.compareTo(_) < 0)
    val list3 = list.sorted
    //反转顺序
    val list4 = list.reverse
    //将list中的元素4个一组,类型为Iterator[List[Int]]
    val list5 = list.grouped(4)
    //将Iterator转换成List
    val list6 = list.toList
    //将多个list压扁成一个List
    val list7 = list5.flatten
    //并行计算求和
    val list8 = list.par.sum
    //化简：reduce
    //将非特定顺序的二元操作应用到所有元素
    val list9 = list.reduce(_ + _)
    //按照特定的顺序
    val list10 = list.reduceLeft(_ + _)
    //折叠：有初始值（无特定顺序）
    val list11 = list.fold(10)(_ + _)
    //折叠：有初始值（有特定顺序）
    val list12 = list.foldLeft(10)(_ + _)

    val lines = List("hello tom hello jerry", "hello jerry", "hello kitty")
    //先按空格切分，在压平
    //    val words = lines.map(_.split(" ")).flatten
    val words = lines.flatMap(_.split(" "))

    //聚合
    val arr = List(List(1, 2, 3), List(3, 4, 5), List(2), List(0))
    val sum = arr.aggregate(0)(_ + _.sum, _ + _)

    val l1 = List(5, 6, 4, 7)
    val l2 = List(1, 2, 3, 4)
    //求并集
    val l3 = l1.union(l2)
    //求交集
    val l4 = l1.intersect(l2)
    //求差集
    val l5 = l1.diff(l2)
    println(l5)
  }
  
}