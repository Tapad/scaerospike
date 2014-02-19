package com.tapad.aerospike

import com.aerospike.client.async.{MaxCommandAction, AsyncClientPolicy, AsyncClient}
import com.aerospike.client._
import scala.concurrent.{ExecutionContext, Promise, Future}
import com.aerospike.client.listener.{RecordArrayListener, DeleteListener, WriteListener, RecordListener}
import com.aerospike.client.policy.{WritePolicy, QueryPolicy, Policy}
import scala.collection.JavaConverters._

object AerospikeClient {

  /**
   * Constructs a new client.
   *
   * @param hosts hosts to sed from
   * @param settings client settings
   */
  def apply(hosts: Seq[String], settings: ClientSettings = ClientSettings.Default)(implicit executionContext: ExecutionContext): AerospikeClient = {
    new AerospikeClient(hosts.map(parseHost), settings)
  }

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
                      private final val settings: ClientSettings)(implicit val executionContext: ExecutionContext) {

  import AerospikeClient._
  private final val policy = settings.buildClientPolicy()
  private final val client = new AsyncClient(policy, hosts: _*)

  /**
   * Creates a new namespace client for this client.
   */
  def namespace[K, V](name: String,
                      readSettings: ReadSettings = ReadSettings.Default,
                      writeSettings: WriteSettings = WriteSettings.Default)(implicit keyKey: KeyGenerator[K]): Namespace[K, V] = new ClientNamespace[K, V](this, name, readSettings, writeSettings)

  private def extractSingleBin[V](bin: String) : Record => Option[V] = {
    case null => None
    case rec => Option(rec.getValue(bin)).asInstanceOf[Option[V]]
  }
  private def extractMultiBin[V] : Record => Map[String, V] = {
    case null => Map.empty
    case rec => rec.bins.asScala.toMap.asInstanceOf[Map[String, V]]
  }

  private[aerospike] def query[R](policy: QueryPolicy,
                                    namespace: String,
                                    key: Key,
                                    bins: Seq[String] = Seq.empty, extract : Record => R) : Future[R] = {
    val result = Promise[R]()
    val listener = new RecordListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)
      def onSuccess(key: Key, record: Record): Unit = result.success(extract(record))
    }
    try {
      if (bins.isEmpty) client.get(policy, listener, key)
      else client.get(policy, listener, key, bins: _*)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  private[aerospike] def multiQuery[R](policy: QueryPolicy,
                                       namespace: String,
                                       keys: Seq[Key],
                                       bins: Seq[String],
                                       extract : Record => R) : Future[Map[Key, R]] = {
    val result = Promise[Map[Key, R]]()
    val listener = new RecordArrayListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {
        var i = 0
        val size = keys.length
        var data = Map.empty[Key, R]
        while (i < size) {
          data += keys(i) -> extract(records(i))
          i += 1
        }
        result.success(data)
      }
    }
    try {
      if (bins.isEmpty) client.get(policy, listener, keys.toArray)
      else client.get(policy, listener, keys.toArray, bins: _*)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }


  private[aerospike] def get[V](policy: QueryPolicy, namespace: String, key: Key, bin: String = ""): Future[Option[V]] = {
    query(policy, namespace, key, bins = Seq(bin), extractSingleBin(bin))
  }

  private[aerospike] def multiGet[V](policy: QueryPolicy, namespace: String, keys: Seq[Key], bin: String = "") : Future[Map[Key, Option[V]]] = {
    multiQuery(policy, namespace, keys, Seq(bin), extractSingleBin(bin))
  }

  private[aerospike] def multiGetBins[V](policy: QueryPolicy, namespace: String, keys: Seq[Key], bins: Seq[String]) : Future[Map[Key, Map[String, V]]] = {
    multiQuery(policy, namespace, keys, bins, extractMultiBin)
  }

  private[aerospike] def getBins[V](policy: QueryPolicy, namespace: String, key: Key, bins: Seq[String]): Future[Map[String, V]] = {
    query(policy, namespace, key, bins = bins, extractMultiBin)
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
class ClientNamespace[K, V](private final val client: AerospikeClient,
                      name: String,
                      readSettings: ReadSettings,
                      writeSettings: WriteSettings)(implicit keyGen: KeyGenerator[K], executionContext: ExecutionContext) extends Namespace[K, V] {
  private final val queryPolicy = readSettings.buildQueryPolicy()
  private final val writePolicy = writeSettings.buildWritePolicy()

  def get(key: K, set: String = "", bin: String = ""): Future[Option[V]] = {
    client.get[V](queryPolicy, name, keyGen(name, set, key), bin = bin)
  }
  def getBins(key: K, set: String = "", bins: Seq[String]) : Future[Map[String, V]] = {
    client.getBins[V](queryPolicy, name, keyGen(name, set, key), bins = bins)
  }

  def multiGet(keys: Seq[K], set: String = "", bin: String = ""): Future[Map[K, Option[V]]] = {
    client.multiGet[V](queryPolicy, name, keys.map(keyGen(name, set, _)), bin = bin).map { result =>
      result.map { case (key, value) => key.userKey.asInstanceOf[K] -> value }
    }
  }

  def multiGetBins(keys: Seq[K], set: String = "", bins: Seq[String]): Future[Map[K, Map[String, V]]] = {
    client.multiGetBins[V](queryPolicy, name, keys.map(keyGen(name, set, _)), bins = bins).map { result =>
      result.map { case (key, value) => key.userKey.asInstanceOf[K] -> value }
    }
  }


  def put(key: K, value: V, set: String = "", bin: String = "", customTtl: Option[Int] = None): Future[Unit] = {
    val policy = customTtl match {
      case None => writePolicy
      case Some(ttl) => 
        val p = writeSettings.buildWritePolicy()
        p.expiration = ttl
        p
    }
    client.put[V](policy, name, keyGen(name, set, key), value, bin = bin)
  } 

  def delete(key: K, set: String = "", bin: String = "") : Future[Unit] = client.delete(writePolicy, name, keyGen(name, set, key), bin = bin)
}

