package com.repocad.hugin

import java.time.ZonedDateTime

import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage

import scala.collection.JavaConverters

/**
  * A utility class to build [[cern.acet.tracing.output.elasticsearch.ElasticsearchMessage]] with required fields.
  *
  * @param messageFactory A function to generate new ElasticsearchMessages with correct type mapping
  * @param requiredFields The names of the required fields
  */
class MessageBuilder(messageFactory: () => ElasticsearchMessage, requiredFields: Set[String]) {

  val errorKey = "parsingerror"
  val timestampKey = "timestamp"

  /**
    * A utility class to build [[cern.acet.tracing.output.elasticsearch.ElasticsearchMessage]] with required fields.
    *
    * @param messageFactory A function to generate new ElasticsearchMessages with correct type mapping
    * @param requiredFields The names of the required fields
    */
  def this(messageFactory: () => ElasticsearchMessage, requiredFields: java.util.Set[String]) = {
    this(messageFactory, JavaConverters.asScalaSet(requiredFields).toSet)
  }

  implicit private def toJavaMap(map: Map[String, AnyRef]): java.util.Map[String, AnyRef] = {
    scala.collection.JavaConverters.mapAsJavaMap[String, AnyRef](map)
  }

  /**
    * Builds a message with the given data. If some required fields are not present an error is thrown.
    *
    * @param data The key-value data to insert into the message.
    * @return Either a message without errors ([[Right]]) or a message with the field 'parsingerror' defined ([[Left]]).
    */
  def buildWith(data: Map[String, AnyRef]): Either[ElasticsearchMessage, ElasticsearchMessage] = {
    requiredFields.map(field => data.contains(field) match {
      case true => None
      case false => Some(s"Missing field '$field'")
    }).filter(_.isDefined).map(_.get).mkString("; ") match {
      case "" => Right(messageFactory().put(parseTimestamp(data)))
      case error => Left(messageFactory().put(errorKey, error).put(parseTimestamp(data)))
    }
  }

  private def parseTimestamp(data: Map[String, AnyRef]): Map[String, AnyRef] = {
    getTimestamp(data) match {
      case Left(error) => data.get(errorKey) match {
        case Some(oldError) => data.filterKeys(_ != timestampKey).updated(errorKey, s"$oldError; $error")
        case None => data.filterKeys(_ != timestampKey)
      }
      case Right(timestamp) => data.updated(timestampKey, timestamp)
    }
  }

  private def getTimestamp(data: Map[String, AnyRef]): Either[String, ZonedDateTime] = {
    data.get(timestampKey).map(value => try {
      Right(ZonedDateTime.parse(value.toString))
    } catch {
      case e: Exception => Left("Malformed timestamp")
    }).getOrElse(Left("Missing key 'timestamp'"))
  }

}