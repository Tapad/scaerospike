package com.tapad.aerospike

import com.aerospike.client.async.AsyncClient
import com.aerospike.client.listener.{DeleteListener, WriteListener, RecordListener, RecordArrayListener}
import com.aerospike.client._
import com.aerospike.client.policy.QueryPolicy

import scala.concurrent.{Promise, ExecutionContext, Future}
import com.aerospike.client.{AerospikeException, Key}
import scala.collection.JavaConverters._
import scala.collection.breakOut

/**
 * Operations on an Aerospike set in a namespace.
 *
 * @tparam K the key type
 * @tparam V the value type. If you have bins / sets with different types, use AnyRef and cast.
 */
trait AsSetOps[K, V] {
  /**
   * Gets the default bin for a single key.
   */
  def get(key: K, bin: String = ""): Future[Option[V]]

  /**
   * Gets multiple bins for a single key.
   */
  def getBins(key: K, bins: Seq[String]) : Future[Map[String, V]]

  /**
   * Gets the default bin for multiple keys.
   */
  def multiGet(keys: Seq[K], bin: String = ""): Future[Map[K, Option[V]]]

  /**
   * Gets multiple bins for a single key.
   */
  def multiGetBins(keys: Seq[K], bins: Seq[String]): Future[Map[K, Map[String, V]]]

  /**
   * Put a value into a key.
   */
  def put(key: K, value: V, bin: String = "", customTtl: Option[Int] = None): Future[Unit]

  /**
   * Put values into multiple bins for a key.
   */
  def putBins(key: K, values: Map[String, V], customTtl: Option[Int] = None) : Future[Unit]

    /**
   * Delete a key.
   */
  def delete(key: K, bin: String = "") : Future[Unit]
}

/**
 * Represents a set in a namespace tied to a specific client.
 */
private[aerospike] class AsSet[K, V](private final val client: AsyncClient,
                                     namespace: String,
                                     set: String,
                                     readSettings: ReadSettings,
                                     writeSettings: WriteSettings)
                                    (implicit keyGen: KeyGenerator[K], valueMapping: ValueMapping[V], executionContext: ExecutionContext) extends AsSetOps[K, V] {

  private final val queryPolicy = readSettings.buildQueryPolicy()
  private final val writePolicy = writeSettings.buildWritePolicy()

  val genKey = keyGen(namespace, set, (_: K))

  private def extractSingleBin(bin: String, record: Record): Option[V] = record match {
    case null => None
    case rec => Option(valueMapping.fromStoredObject(rec.getValue(bin)))
  }

  private def extractMultiBin(record: Record): Map[String, V] = record match {
    case null => Map.empty
    case rec => {
      val result = Map.newBuilder[String, V]
      val iter = rec.bins.entrySet().iterator()
      while (iter.hasNext) {
        val bin = iter.next()
        val obj = bin.getValue
        if (obj != null) result += bin.getKey -> valueMapping.fromStoredObject(obj)
      }
      result.result()
    }
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
      if (bins.isEmpty) client.get(policy, listener, key)
      else client.get(policy, listener, key, bins: _*)
    } catch {
      case e: Throwable => result.failure(e)
    }
    result.future
  }

  private[aerospike] def multiQuery[T](policy: QueryPolicy,
                                       keys: Seq[Key],
                                       bins: Seq[String],
                                       extract: Record => T): Future[Map[K, T]] = {
    val result = Promise[Map[K, T]]()
    val listener = new RecordArrayListener {
      def onFailure(exception: AerospikeException): Unit = result.failure(exception)

      def onSuccess(keys: Array[Key], records: Array[Record]): Unit = {
        var i = 0
        val size = keys.length
        var data = Map.newBuilder[K, T]
        while (i < size) {
          data += keys(i).userKey.getObject.asInstanceOf[K] -> extract(records(i))
          i += 1
        }
        result.success(data.result())
      }
    }
    try {
      if (bins.isEmpty) client.get(policy, listener, keys.toArray)
      else client.get(policy, listener, keys.toArray, bins: _*)
    } catch {
      case e: Throwable => result.failure(e)
    }
    result.future
  }

  def get(key: K, bin: String = ""): Future[Option[V]] = {
    query(queryPolicy, genKey(key), bins = Seq(bin), extractSingleBin(bin, _))
  }

  def getBins(key: K, bins: Seq[String]): Future[Map[String, V]] = {
    query(queryPolicy, genKey(key), bins = bins, extractMultiBin)
  }

  def multiGet(keys: Seq[K], bin: String = ""): Future[Map[K, Option[V]]] = {
    multiQuery(queryPolicy, keys.map(genKey), bins = Seq(bin), extractSingleBin(bin, _))
  }

  def multiGetBins(keys: Seq[K], bins: Seq[String]): Future[Map[K, Map[String, V]]] = {
    multiQuery(queryPolicy, keys.map(genKey), bins, extractMultiBin)
  }

  def put(key: K, value: V, bin: String = "", customTtl: Option[Int] = None): Future[Unit] = {
    putBins(key, Map(bin -> value), customTtl)
  }

  def putBins(key: K, values: Map[String, V], customTtl: Option[Int] = None) : Future[Unit] = {
    val policy = customTtl match {
      case None => writePolicy
      case Some(ttl) =>
        val p = writeSettings.buildWritePolicy()
        p.expiration = ttl
        p
    }
    val bins: Array[Bin] = values.map { case (binName, binValue) => new Bin(binName, binValue) }(breakOut)
    val result = Promise[Unit]()
    try {
      val listener = new WriteListener {
        def onFailure(exception: AerospikeException) { result.failure(exception) }

        def onSuccess(key: Key) { result.success(Unit) }
      }
      client.put(policy, listener, genKey(key), bins: _*)
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }

  def delete(key: K, bin: String = ""): Future[Unit] = {
    val result = Promise[Unit]()
    try {
      val listener = new DeleteListener {
        def onFailure(exception: AerospikeException) {
          result.failure(exception)
        }

        def onSuccess(key: Key, existed: Boolean) {
          result.success(Unit)
        }
      }
      client.delete(writePolicy, listener, genKey(key))
    } catch {
      case e: com.aerospike.client.AerospikeException => result.failure(e)
    }
    result.future
  }
}


