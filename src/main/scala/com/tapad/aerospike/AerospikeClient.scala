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
                writeSettings: WriteSettings = WriteSettings.Default): AsNamespace = new AsNamespace(this, name, readSettings, writeSettings)

  private def extractSingleBin[V](bin: String): Record => Option[V] = {
    case null => None
    case rec => Option(rec.getValue(bin)).asInstanceOf[Option[V]]
  }

  private def extractMultiBin[V]: Record => Map[String, V] = {
    case null => Map.empty
    case rec => rec.bins.asScala.toMap.asInstanceOf[Map[String, V]]
  }

  private[aerospike] def query[R](policy: QueryPolicy,
                                  key: Key,
                                  bins: Seq[String] = Seq.empty, extract: Record => R): Future[R] = {
    val result = Promise[R]()
    val listener = new RecordListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(key: Key, record: Record): Unit = result.success(extract(record))
    }
    try {
      if (bins.isEmpty) underlying.get(policy, listener, key)
      else underlying.get(policy, listener, key, bins: _*)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  private[aerospike] def multiQuery[R](policy: QueryPolicy,
                                       keys: Seq[Key],
                                       bins: Seq[String],
                                       extract: Record => R): Future[Map[Key, R]] = {
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
      if (bins.isEmpty) underlying.get(policy, listener, keys.toArray)
      else underlying.get(policy, listener, keys.toArray, bins: _*)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }


  private[aerospike] def get[V](policy: QueryPolicy, key: Key, bin: String = ""): Future[Option[V]] = {
    query(policy, key, bins = Seq(bin), extractSingleBin(bin))
  }

  private[aerospike] def multiGet[V](policy: QueryPolicy, keys: Seq[Key], bin: String = ""): Future[Map[Key, Option[V]]] = {
    multiQuery(policy, keys, Seq(bin), extractSingleBin(bin))
  }

  private[aerospike] def multiGetBins[V](policy: QueryPolicy, keys: Seq[Key], bins: Seq[String]): Future[Map[Key, Map[String, V]]] = {
    multiQuery(policy, keys, bins, extractMultiBin)
  }

  private[aerospike] def getBins[V](policy: QueryPolicy, key: Key, bins: Seq[String]): Future[Map[String, V]] = {
    query(policy, key, bins = bins, extractMultiBin)
  }

  private[aerospike] def put[V](policy: WritePolicy, key: Key, value: V, bin: String = ""): Future[Unit] = {
    val b = new Bin(bin, value)
    val result = Promise[Unit]()
    try {
      underlying.put(policy, new WriteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key) { result.success(Unit) }
      }, key, b)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  private[aerospike] def delete(policy: WritePolicy, key: Key, bin: String = ""): Future[Unit] = {
    val result = Promise[Unit]()
    try {
      underlying.delete(policy, new DeleteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key, existed: Boolean) { result.success(Unit) }
      }, key)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

}

