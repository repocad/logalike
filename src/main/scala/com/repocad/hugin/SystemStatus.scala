package com.repocad.hugin

import java.io.File

import org.apache.logging.log4j.LogManager

import scala.collection.JavaConverters
import scala.io.Source

/**
  * Collects system stats
  */
object SystemStatus {

  private val logger = LogManager.getLogger(SystemStatus)

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
      .map(tuple => tuple(0) -> parseField(tuple(1), file)).toMap
  }

  private def parseField(input: String, file: File): AnyRef = {
    try {
      // If the string is not empty and the first character is a digit, assume it's a number
      if (input.nonEmpty && input.charAt(0).isDigit) {
        Integer.decode(input.takeWhile(_ != ' '))
      } else {
        input
      }
    } catch {
      case e: Exception =>
        logger.info(s"Failed to parse number in  $file from '$input', defaulting to string")
        input
    }
  }

}
