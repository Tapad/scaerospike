package com.tapad.aerospike

import scala.concurrent.{ExecutionContext, Future}
import com.aerospike.client.Key

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
   * Delete a key.
   */
  def delete(key: K, bin: String = "") : Future[Unit]
}

/**
 * Represents a set in a namespace tied to a specific client.
 */
private[aerospike] class AsSet[K, V](private final val client: AerospikeClient,
                                     namespace: String,
                                     set: String,
                                     readSettings: ReadSettings,
                                     writeSettings: WriteSettings)
                                    (implicit keyGen: KeyGenerator[K], executionContext: ExecutionContext) extends AsSetOps[K, V] {

  private final val queryPolicy = readSettings.buildQueryPolicy()
  private final val writePolicy = writeSettings.buildWritePolicy()

  def get(key: K, bin: String = ""): Future[Option[V]] = {
    client.get[V](queryPolicy, keyGen(namespace, set, key), bin = bin)
  }

  def getBins(key: K, bins: Seq[String]): Future[Map[String, V]] = {
    client.getBins[V](queryPolicy, keyGen(namespace, set, key), bins = bins)
  }

  def multiGet(keys: Seq[K], bin: String = ""): Future[Map[K, Option[V]]] = {
    client.multiGet[V](queryPolicy, keys.map(keyGen(namespace, set, _)), bin = bin).map { result =>
      result.map { case (key, value) => key.userKey.asInstanceOf[K] -> value}
    }
  }

  def multiGetBins(keys: Seq[K], bins: Seq[String]): Future[Map[K, Map[String, V]]] = {
    client.multiGetBins[V](queryPolicy, keys.map(keyGen(namespace, set, _)), bins = bins).map { result =>
      result.map { case (key, value) => key.userKey.asInstanceOf[K] -> value}
    }
  }


  def put(key: K, value: V, bin: String = "", customTtl: Option[Int] = None): Future[Unit] = {
    val policy = customTtl match {
      case None => writePolicy
      case Some(ttl) =>
        val p = writeSettings.buildWritePolicy()
        p.expiration = ttl
        p
    }
    client.put[V](policy, keyGen(namespace, set, key), value, bin = bin)
  }

  def delete(key: K, bin: String = ""): Future[Unit] = client.delete(writePolicy, keyGen(namespace, set, key), bin = bin)
}


