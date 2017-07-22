package com.xz.akka.test

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

class Master extends Actor {
  override def preStart(): Unit = {
    println("preStart invoke")
  }

  override def receive: Receive = {
    case "connect" => {
      println("client connect")
    }
    case "hello" => {
      println("hi")
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1)
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem("MasterSystem", config)
    val master = actorSystem.actorOf(Props[Master], "Master")
    master ! "hello"
    actorSystem.awaitTermination()
  }
}