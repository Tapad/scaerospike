package com.tapad.aerospike

import com.aerospike.client.async.{MaxCommandAction, AsyncClientPolicy, AsyncClient}
import com.aerospike.client._
import scala.concurrent.{Promise, Future}
import com.aerospike.client.listener.{WriteListener, RecordListener}
import com.aerospike.client.policy.{WritePolicy, QueryPolicy, Policy}


object AerospikeClient {

  /**
   * Constructs a new client.
   *
   * @param hosts hosts to sed from
   * @param settings client settings
   */
  def apply(hosts: Seq[String], settings: ClientSettings = ClientSettings.Default): AerospikeClient = new AerospikeClient(hosts.map(parseHost), settings)

  def parseHost(hostString: String) = hostString.split(':') match {
    case Array(host, port) => new Host(host, port.toInt)
    case Array(host) => new Host(host, 3000)
    case _ => throw new IllegalArgumentException("Invalid host string %s".format(hostString))
  }

}

/**
 * Wraps the Java client and exposes methods to build clients for individual namespaces.
 *
 * @param hosts the servers hosts
 * @param settings the settings for this client
 */
class AerospikeClient(private val hosts: Seq[Host],
                      private final val settings: ClientSettings) {

  private final val policy = settings.buildClientPolicy()
  private final val client = new AsyncClient(policy, hosts: _*)

  /**
   * Creates a new namespace client for this client.
   */
  def namespace[K, V](name: String,
                      readSettings: ReadSettings = ReadSettings.Default,
                      writeSettings: WriteSettings = WriteSettings.Default): Namespace[K, V] = new Namespace[K, V](this, name, readSettings, writeSettings)

  private[aerospike] def get[K, V](policy: QueryPolicy, namespace: String, key: K, set: String = "", bin: String = ""): Future[Option[V]] = {
    val k = new Key(namespace, set, key)
    val result = Promise[Option[V]]()
    try {
      client.get(policy, new RecordListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key, record: Record) {
          val value: Option[V] = record match {
            case null => None
            case rec => Option(rec.getValue(bin)).asInstanceOf[Option[V]]
          }
          result.success(value)
        }
      }, k)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  private[aerospike] def put[K, V](policy: WritePolicy, namespace: String, key: K, value: V, set: String = "", bin: String = ""): Future[Unit] = {
    val k = new Key(namespace, set, key)
    val b = new Bin(bin, value)
    val result = Promise[Unit]()
    try {
      client.put(policy, new WriteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key) { result.success(Unit) }
      }, k, b)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

}

/**
 * Represents a namespace tied to a specific client. This is the main entry-point for most
 * client operations.
 * @param client the client to use
 * @param name the name of the namespace
 * @param readSettings settings for reads
 * @param writeSettings settings for writes
 * @tparam K the key type for this namespace
 * @tparam V the value type for this namespace
 */
class Namespace[K, V](private final val client: AerospikeClient,
                      name: String,
                      readSettings: ReadSettings,
                      writeSettings: WriteSettings) {
  private final val queryPolicy = readSettings.buildQueryPolicy()
  private final val writePolicy = writeSettings.buildWritePolicy()

  def get(key: K, set: String = "", bin: String = ""): Future[Option[V]] = client.get[K, V](queryPolicy, name, key, set = set, bin = bin)

  def put(key: K, value: V, set: String = "", bin: String = ""): Future[Unit] = client.put[K, V](writePolicy, name, key, value, set = set, bin = bin)
}

/**
 * Aerospike client settings.
 *
 * @param maxCommandsOutstanding the maximum number of outstanding commands before rejections will happen
 */
case class ClientSettings(maxCommandsOutstanding: Int = 500, selectorThreads: Int = 1) {

  /**
   * @return a mutable policy object for the Java client.
   */
  private[aerospike] def buildClientPolicy() = {
    val p = new AsyncClientPolicy()
    p.asyncMaxCommandAction = MaxCommandAction.REJECT
    p.asyncMaxCommands      = maxCommandsOutstanding
    p.asyncSelectorThreads  = selectorThreads
    p
  }
}

object ClientSettings {
  val Default = ClientSettings()
}


case class ReadSettings(timeout: Int = 0, maxRetries: Int = 2, sleepBetweenRetries: Int = 500, maxConcurrentNodes: Int = 0) {
  private[aerospike] def buildQueryPolicy() = {
    val p = new QueryPolicy()
    p.timeout             = timeout
    p.maxRetries          = maxRetries
    p.sleepBetweenRetries = sleepBetweenRetries
    p.maxConcurrentNodes  = maxConcurrentNodes
    p
  }
}

object ReadSettings {
  val Default = ReadSettings()
}

case class WriteSettings(expiration: Int = 0, timeout: Int = 0, maxRetries: Int = 2, sleepBetweenRetries: Int = 500) {
  private[aerospike] def buildWritePolicy() = {
    val p = new WritePolicy()
    p.expiration = expiration
    p.timeout = timeout
    p.maxRetries = maxRetries
    p.sleepBetweenRetries = sleepBetweenRetries
    p
  }
}

object WriteSettings {
  val Default = WriteSettings()
}

