package com.xz.akka.test

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Woker(val masterHost: String, val masterPort: Int) extends Actor {

  var master: ActorSelection = _

  override def preStart(): Unit = {
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master")
    master ! "connect"
  }

  override def receive: Receive = {
    case "reply" => {
      println("reply from Master")
    }
  }

}

object Woker {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val wokerSystem = ActorSystem("WorkerSystem", config)
    wokerSystem.actorOf(Props(new Woker(masterHost, masterPort)), "Worker")
    wokerSystem.awaitTermination()
  }
}