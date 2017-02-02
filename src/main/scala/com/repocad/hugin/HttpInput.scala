package com.repocad.hugin

import java.util.concurrent.ArrayBlockingQueue
import java.util.function.{Predicate, Supplier}
import java.util.stream.Stream

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{HttpOriginRange, `Access-Control-Allow-Origin`}
import akka.http.scaladsl.server.{Directives, ExceptionHandler, RouteResult}
import akka.stream.ActorMaterializer
import cern.acet.tracing.CloseableInput
import cern.acet.tracing.output.elasticsearch.ElasticsearchMessage
import org.apache.logging.log4j.LogManager
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

  private val logger = LogManager.getLogger(HttpInput)

  val allowedOrigins: HttpOriginRange = HttpOriginRange("*")

  def apply(host: String, messageBuilder: MessageBuilder): HttpInput = {
    implicit val system = ActorSystem("hugin")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    import system.dispatcher

    val queue: ArrayBlockingQueue[ElasticsearchMessage] = new ArrayBlockingQueue[ElasticsearchMessage](1000)

    val bindingFuture = run(queue, host, messageBuilder)
    new HttpInput(queue, () => bindingFuture.flatMap(_.unbind()).onComplete(_ => system.terminate()))
  }

  private def run(queue: ArrayBlockingQueue[ElasticsearchMessage], host: String, messageBuilder: MessageBuilder)
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
            respondWithHeader(`Access-Control-Allow-Origin`.forRange(allowedOrigins)) {
              entity(as[String]) { body =>
                val map = body.parseJson.convertTo[Map[String, JsValue]]
                val entry = map.map(t => t._1 -> AnyJsonFormat.read(t._2).asInstanceOf[AnyRef])
                val message = messageBuilder.buildWith(entry)

                queue.put(message.merge)

                // Output error if parsing error
                message match {
                  case Left(error) => complete(HttpResponse(StatusCodes.BadRequest, entity = error.get("parsingerror").toString))
                  case Right(_) => complete(HttpResponse(StatusCodes.OK))
                }
              }
            }
          }
        }
      }

    val (hostName, port) = extractHostAndPort(host)

    logger.info(s"Starting HTTP server at $hostName:$port")
    Http().bindAndHandle(RouteResult.route2HandlerFlow(route), hostName, port)
  }

  private def extractHostAndPort(host: String): (String, Int) = {
    try {
      host.split(":").toList match {
        case hostName :: port :: Nil => (hostName, Integer.parseInt(port))
        case e => throw new IllegalArgumentException(host)
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to parse binding host and port. Defaulting to localhost:8080", e)
        ("localhost", 8080)
    }
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