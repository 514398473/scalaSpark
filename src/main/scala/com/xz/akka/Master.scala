package com.xz.akka

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._

class Master(val host: String, val port: Int) extends Actor {
  val idToWorker = new mutable.HashMap[String, WorkInfo]()
  val workers = new mutable.HashSet[WorkInfo]()

  override def preStart(): Unit = {
    println("preStart invoke")
    import context.dispatcher
    context.system.scheduler.schedule(0 millis, 15000 millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {
    case RegisterWorker(id, memory, core) => {
      if (!idToWorker.contains(id)) {
        val wokerInfo = new WorkInfo(id, memory, core)
        idToWorker(id) = wokerInfo
        workers += wokerInfo
        sender ! RegisteredWorker(s"akka.tcp://MasterSystem@$host:$port/user/Master")
      }
    }
    case HeartBeat(id) => {
      if (idToWorker.contains(id)) {
        val wokerInfo = idToWorker(id)
        val currentTime = System.currentTimeMillis()
        wokerInfo.lastHeartbeatTime = currentTime
      }
    }
    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      val toRemove = workers.filter(x => currentTime - x.lastHeartbeatTime > 15000)
      for (t <- toRemove) {
        idToWorker -= t.id
        workers -= t
      }
      println(workers.size)
    }
  }
}

object Master {
  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    val actorSystem = ActorSystem("MasterSystem", config)
    val master = actorSystem.actorOf(Props(new Master(host, port)), "Master")
    actorSystem.awaitTermination()
  }
}