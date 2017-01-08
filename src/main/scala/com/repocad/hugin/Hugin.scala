package com.repocad.hugin

import java.io.{PrintWriter, StringWriter}
import java.time.{Duration, Instant, ZonedDateTime}
import java.util.concurrent.{Executors, TimeUnit}

import cern.acet.tracing.Logalike
import cern.acet.tracing.output.ElasticsearchOutput
import cern.acet.tracing.output.elasticsearch.{ElasticsearchIndex, ElasticsearchMessage, ElasticsearchStaticMapping}
import cern.acet.tracing.util.`type`.strategy.AcceptStrategy
import com.google.common.collect.ImmutableMap

import scala.concurrent.{Future, JavaConversions}
import scala.io.StdIn

/**
  * Hugin is one of the ravens delivering messages to Odin.
  */
object Hugin {

  private final val start = Instant.now()

  def main(args: Array[String]): Unit = {
    val (elastiscearchHost, bindingHost) = args.length match {
      case 0 =>
        println("No host for elasticsearch or binding given, defaulting to localhost:9300 serving at localhost:8080")
        ("localhost:9300", "localhost:8080")
      case 1 =>
        println("No host for local binding given, defaulting to localhost:8080")
        (args(0), "localhost:8080")
      case e =>
        if (e > 2) {
          println("Alright, stop with the arguments already!!")
        }
        (args(0), args(1))
    }

    // Define required fields and the type mapping for Elasticsearch
    val requiredFields = ImmutableMap
      .builder[String, Class[_]]()
      .put("timestamp", classOf[ZonedDateTime])
      .put("level", classOf[String])
      .put("ip", classOf[String])
      .put("version", classOf[String])
      .put("product", classOf[String])
      .put("service", classOf[String])
      .build()
    val typeMapping = new ElasticsearchStaticMapping(requiredFields)

    // Define the output to Elasticsearch and connect to the cluster
    val output = ElasticsearchOutput.builder()
      .addHost(elastiscearchHost)
      .setClusterName("odin")
      .setDefaultIndex(ElasticsearchIndex.daily("logalike"))
      .setNodeName("hugin")
      .setMapping(typeMapping)
      .setTypeStrategy(AcceptStrategy.INSTANCE)
      .setFlushInterval(Duration.ofSeconds(10))
      .build()

    // Send a status message every minute
    val statusScheduler = Executors.newSingleThreadScheduledExecutor()
    statusScheduler.scheduleAtFixedRate(new StatusDispatcher(output), 0, 1, TimeUnit.MINUTES)

    // Create the input
    val messageBuilder = new MessageBuilder(() => output.createTypedMessage(), requiredFields.keySet())
    val input = HttpInput(bindingHost, messageBuilder)

    // Start Logalike in a separate thread
    implicit val context = JavaConversions.asExecutionContext(Executors.newSingleThreadExecutor())
    val logalike = Logalike.builder()
      .setInput(input)
      .setOutput(output)
      .build()
    Future {
      logalike.run()
    }

    StdIn.readLine()
    logalike.close()
    statusScheduler.shutdownNow()
    context.shutdownNow()
  }

}

private class StatusDispatcher(output: ElasticsearchOutput) extends Runnable {

  override def run(): Unit = {
    output.accept(statusMessage(output))
  }

  private def statusMessage(output: ElasticsearchOutput): ElasticsearchMessage = {
    val message = output.createTypedMessage().put("level", "info")
      .put("ip", "0.0.0.0")
      .put("version", "0.1")
      .put("product", "hugin")
      .put("service", "hugin")
      .put("messagessent", output.getMessageCounter)

    try {
      message.put(SystemStatus.collect())
    } catch {
      case e: Exception =>
        val writer = new StringWriter()
        val printer = new PrintWriter(writer)
        e.printStackTrace(printer)
        printer.flush()
        message.put("body", "Error while collecting data")
          .put("exception", writer.toString)
    }
  }
}
