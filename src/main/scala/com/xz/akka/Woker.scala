package com.xz.akka

import java.util.UUID
import scala.concurrent.duration._
import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Woker(val masterHost: String, val masterPort: Int, val memory: Int, val core: Int) extends Actor {
  val id = UUID.randomUUID().toString
  var master: ActorSelection = _

  override def preStart(): Unit = {
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    master ! RegisterWorker(id, memory, core)
  }

  override def receive: Receive = {
    case RegisteredWorker(masterUrl) => {
      println(masterUrl)
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, 10000 millis, self, SendHeartbeat)
    }
    case SendHeartbeat => {
      println("send heartbeat to master")
      master ! HeartBeat(id)
    }
  }

}

object Woker {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val memory = args(4).toInt
    val core = args(5).toInt
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val wokerSystem = ActorSystem("WorkerSystem", config)
    wokerSystem.actorOf(Props(new Woker(masterHost, masterPort, memory, core)), "Worker")
    wokerSystem.awaitTermination()
  }
}