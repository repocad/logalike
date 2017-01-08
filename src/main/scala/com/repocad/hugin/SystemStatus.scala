package com.repocad.hugin

import java.io.File

import scala.collection.JavaConverters
import scala.io.Source

/**
  * Collects system stats
  */
object SystemStatus {

  def collect(): java.util.Map[String, AnyRef] = {
    import JavaConverters.mapAsJavaMap
    val meminfo = parseInfoFile(new File("/proc/meminfo")).map(t => "mem_" + t._1 -> t._2)
    val cpuinfo = parseInfoFile(new File("/proc/cpuinfo")).map(t => "cpu_" + t._1 -> t._2)
    mapAsJavaMap(meminfo ++ cpuinfo)
  }

  private def parseInfoFile(file: File): Map[String, AnyRef] = {
    Source.fromFile(file)
      .getLines()
      .map(_.split(":").map(_.trim))
      .filter(_.length > 1)
      .map(tuple => tuple(0) -> tuple(1)).toMap
  }


}
