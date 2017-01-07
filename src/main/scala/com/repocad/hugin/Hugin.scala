package com.repocad.hugin

import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.Executors

import cern.acet.tracing.Logalike
import cern.acet.tracing.output.ElasticsearchOutput
import cern.acet.tracing.output.elasticsearch.ElasticsearchIndex
import cern.acet.tracing.util.`type`.strategy.{AcceptStrategy, ThrowingStrategy}
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.{InetSocketTransportAddress, TransportAddress}

import scala.concurrent.{Future, JavaConversions}
import scala.io.StdIn

/**
  * Hugin is one of the ravens delivering messages to Odin.
  */
object Hugin {

  def main(args: Array[String]): Unit = {
    val output = ElasticsearchOutput.builder()
      .addHost("localhost:9300")
      .setClusterName("odin")
      .setDefaultIndex(ElasticsearchIndex.daily("logalike"))
      .setNodeName("hugin")
      .setTypeStrategy(AcceptStrategy.INSTANCE)
      .setFlushInterval(Duration.ofSeconds(10))
      .build()

    val input = HttpInput("localhost", 8080, () => output.createTypedMessage())

    implicit val context = JavaConversions.asExecutionContext(Executors.newSingleThreadExecutor())
    Future {
      Logalike.builder()
        .setInput(input)
        .setOutput(output)
        .build()
        .run()
    }

    StdIn.readLine()
    context.shutdownNow()
  }

}
