package com.tapad.aerospike

import com.aerospike.client.async.AsyncClient
import com.aerospike.client.Host
import scala.concurrent.ExecutionContext

object AerospikeClient {

  /**
   * Constructs a new client.
   *
   * @param hosts hosts to sed from
   * @param settings client settings
   */
  def apply(hosts: Seq[String], settings: ClientSettings = ClientSettings.Default)(implicit executionContext: ExecutionContext): AerospikeClient = {
    val policy = settings.buildClientPolicy()
    val underlying = new AsyncClient(policy, hosts.map(parseHost): _*)
    new AerospikeClient(underlying)
  }

  def parseHost(hostString: String) = hostString.split(':') match {
    case Array(host, port) => new Host(host, port.toInt)
    case Array(host) => new Host(host, 3000)
    case _ => throw new IllegalArgumentException("Invalid host string %s".format(hostString))
  }
}

/**
 * Wraps the Java client and exposes methods to build clients for individual namespaces.
 */
class AerospikeClient(private final val underlying: AsyncClient) {

  /**
   * Creates a new namespace client for this client.
   */
  def namespace(name: String,
                readSettings: ReadSettings = ReadSettings.Default,
                writeSettings: WriteSettings = WriteSettings.Default): AsNamespace = new AsNamespace(underlying, name, readSettings, writeSettings)
}

