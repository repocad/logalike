package com.repocad.hugin

import java.time.ZonedDateTime
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.{Predicate, Supplier}
import java.util.stream.Stream

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.{Directives, ExceptionHandler, RouteResult}
import akka.stream.ActorMaterializer
import cern.acet.tracing.CloseableInput
import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage
import spray.json.{DefaultJsonProtocol, JsArray, JsFalse, JsNull, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, JsonParser}

import scala.concurrent.Future

sealed class HttpInput(private val queue: ArrayBlockingQueue[ElasticsearchMessage], private val closeFunction: () => Unit) extends CloseableInput[ElasticsearchMessage] {

  override def get(): Stream[ElasticsearchMessage] = {
    val supplier: Supplier[Option[ElasticsearchMessage]] = () => {
      try {
        Some(queue.take())
      } catch {
        case e: NoSuchElementException => None
      }
    }
    val predicate: Predicate[Option[ElasticsearchMessage]] = (option: Option[ElasticsearchMessage]) => option.isDefined
    val mapper: java.util.function.Function[Option[ElasticsearchMessage], ElasticsearchMessage] = (option: Option[ElasticsearchMessage]) => option.get
    Stream.generate[Option[ElasticsearchMessage]](supplier).filter(predicate).map[ElasticsearchMessage](mapper)
  }

  override def close(): Unit = closeFunction()

}

object HttpInput extends Directives with DefaultJsonProtocol {

  val timestampKey = "timestamp"

  def apply(host: String, port: Int, messageFactory: () => ElasticsearchMessage): HttpInput = {
    implicit val system = ActorSystem("hugin")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    import system.dispatcher

    val queue: ArrayBlockingQueue[ElasticsearchMessage] = new ArrayBlockingQueue[ElasticsearchMessage](1000)

    val bindingFuture = run(queue, host, port, messageFactory)
    new HttpInput(queue, () => bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate()))
  }

  private def run(queue: ArrayBlockingQueue[ElasticsearchMessage], host: String, port: Int, messageFactory: () => ElasticsearchMessage)
                 (implicit system: ActorSystem, materializer: ActorMaterializer): Future[Http.ServerBinding] = {
    val exceptionHandler = ExceptionHandler {
      case e: JsonParser.ParsingException =>
        complete(HttpResponse(StatusCodes.BadRequest, entity = "Malformed JSON"))
    }

    import spray.json._

    val route =
      handleExceptions(exceptionHandler) {
        path("") {
          put {
            entity(as[String]) { body =>
              val map = body.parseJson.convertTo[Map[String, JsValue]]
              val entry = map.map(t => t._1 -> AnyJsonFormat.read(t._2).asInstanceOf[AnyRef])
              val javaMap = scala.collection.JavaConverters.mapAsJavaMap[String, AnyRef](entry)

              val message = messageFactory()

              val elasticsearchMessage = entry.get(timestampKey).map(value => try {
                Right(ZonedDateTime.parse(value.toString))
              } catch {
                case e: Exception => Left("Malformed timestamp")
              }) match {
                case Some(Right(timestamp)) => message.put(javaMap).put(timestampKey, timestamp)
                case Some(Left(error)) => message.put(javaMap).put("parsingerror", error)
                case None => message.put(javaMap).put("parsingerror", "No timestamp given")
              }

              queue.put(elasticsearchMessage)

              // Output error if parsing error
              if (elasticsearchMessage.containsKey("parsingerror")) {
                complete(HttpResponse(StatusCodes.BadRequest, entity = elasticsearchMessage.get("parsingerror").toString))
              } else {
                complete(HttpResponse(StatusCodes.OK))
              }
            }
          }
        }
      }

    Http().bindAndHandle(RouteResult.route2HandlerFlow(route), host, port)
  }

}

object AnyJsonFormat extends JsonFormat[Any] {
  def write(x: Any): JsValue = x match {
    case d: Double => JsNumber(d)
    case l: Long => JsNumber(l)
    case n: Int => JsNumber(n)
    case s: String => JsString(s)
    case b: Boolean if b => JsTrue
    case b: Boolean => JsFalse
    case a: Array[Any] => JsArray(a.map(AnyJsonFormat.write).toVector)
    case m: Map[String, Any] => JsObject(m.map(t => t._1.toString -> write(t._2)))
  }

  def read(value: JsValue): Any = value match {
    case JsNumber(n) => n.doubleValue()
    case JsString(s) => s
    case JsTrue => true
    case JsFalse => false
    case JsObject(map) => map.map(t => t._1 -> read(t._2))
    case JsArray(list) => list.map(read)
    case JsNull => Option.empty[Any]
  }
}