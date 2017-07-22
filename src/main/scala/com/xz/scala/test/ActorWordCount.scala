package com.xz.scala.test

import scala.actors.{Actor, Future}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

class Task extends Actor {
  override def act(): Unit = {
    loop {
      react {
        case SubmitTask(fileName) => {
          val result = Source.fromFile(fileName).getLines().flatMap(_.split(" ")).map((_, 1)).toList.groupBy(_._1).mapValues(_.size)
          sender ! ResultTask(result)
        }
        case StopTask => {
          exit()
        }
      }
    }
  }

}

case class SubmitTask(fileName: String)

case class ResultTask(result: Map[String, Int])

case object StopTask

object ActorWordCount {

  def main(args: Array[String]): Unit = {
    val replySet = new mutable.HashSet[Future[Any]]()
    val resultList = new ListBuffer[ResultTask]()
    val files = Array("C:\\Users\\Administrator\\Desktop\\1.txt", "C:\\Users\\Administrator\\Desktop\\2.txt")
    for (file <- files) {
      val task = new Task
      task.start()
      val reply = task !! SubmitTask(file)
      replySet += reply
    }

    while (replySet.size > 0) {
      val complete = replySet.filter(_.isSet)
      for (c <- complete) {
        val result = c().asInstanceOf[ResultTask]
        resultList += result
        replySet -= c
      }
      Thread.sleep(1000)
    }
    val finalResult = resultList.flatMap(_.result).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
    println(finalResult)
  }

}

