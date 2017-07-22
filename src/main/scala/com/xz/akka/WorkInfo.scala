package com.xz.akka

class WorkInfo(val id: String, val memory: Int, val core: Int) {
  var lastHeartbeatTime: Long = _
}
