package com.xz.akka

trait RemoteMessage extends Serializable

case class RegisterWorker(id: String, memory: Int, core: Int) extends RemoteMessage

case class HeartBeat(id: String) extends RemoteMessage

case class RegisteredWorker(masterUrl: String) extends RemoteMessage

case object CheckTimeOutWorker

case object SendHeartbeat


