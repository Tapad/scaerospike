package com.tapad.aerospike

import com.aerospike.client.async.{MaxCommandAction, AsyncClientPolicy, AsyncClient}
import com.aerospike.client._
import scala.concurrent.{Promise, Future}
import com.aerospike.client.listener.{DeleteListener, WriteListener, RecordListener}
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

  import AerospikeClient._
  private final val policy = settings.buildClientPolicy()
  private final val client = new AsyncClient(policy, hosts: _*)

  /**
   * Creates a new namespace client for this client.
   */
  def namespace[K, V](name: String,
                      readSettings: ReadSettings = ReadSettings.Default,
                      writeSettings: WriteSettings = WriteSettings.Default)(implicit keyKey: KeyGenerator[K]): Namespace[K, V] = new Namespace[K, V](this, name, readSettings, writeSettings)

  private[aerospike] def get[V](policy: QueryPolicy, namespace: String, key: Key, bin: String = ""): Future[Option[V]] = {
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
      }, key)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  private[aerospike] def put[V](policy: WritePolicy, namespace: String, key: Key, value: V, bin: String = ""): Future[Unit] = {
    val b = new Bin(bin, value)
    val result = Promise[Unit]()
    try {
      client.put(policy, new WriteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key) { result.success(Unit) }
      }, key, b)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  private[aerospike] def delete(policy: WritePolicy, namespace: String, key: Key, bin: String = "") : Future[Unit] = {
    val result = Promise[Unit]()
    try {
      client.delete(policy, new DeleteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }
        def onSuccess(key: Key, existed: Boolean) { result.success(Unit) }
      }, key)
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
                      writeSettings: WriteSettings)(implicit keyGen: KeyGenerator[K]) {
  private final val queryPolicy = readSettings.buildQueryPolicy()
  private final val writePolicy = writeSettings.buildWritePolicy()

  def get(key: K, set: String = "", bin: String = ""): Future[Option[V]] = client.get[V](queryPolicy, name, keyGen(name, set, key), bin = bin)

  def put(key: K, value: V, set: String = "", bin: String = ""): Future[Unit] = client.put[V](writePolicy, name, keyGen(name, set, key), value, bin = bin)

  def delete(key: K, set: String = "", bin: String = "") : Future[Unit] = client.delete(writePolicy, name, keyGen(name, set, key), bin = bin)
}

/**
 * The AS client no longer supports AnyRef keys, we need a way to create keys from values.
 * We define this as a separate case class wrapping the generator function in order to define
 * the implicit implementations in the companion.
 */
case class KeyGenerator[K](gen: (String, String, K) => Key) {
  final def apply(ns: String, set: String, key: K) : Key = gen(ns, set, key)
}

object KeyGenerator {
  implicit val StringKeyGenerator : KeyGenerator[String] = KeyGenerator((ns: String, set: String, key: String) => new Key(ns, set, key))
  implicit val IntKeyGenerator : KeyGenerator[Int] = KeyGenerator((ns: String, set: String, key: Int) => new Key(ns, set, key))
  implicit val ByteArrayKeyGenerator : KeyGenerator[Array[Byte]] = KeyGenerator((ns: String, set: String, key: Array[Byte]) => new Key(ns, set, key))
  implicit val LongKeyGenerator : KeyGenerator[Long] = KeyGenerator((ns: String, set: String, key: Long) => new Key(ns, set, key))
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

