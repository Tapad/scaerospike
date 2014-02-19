package com.tapad.aerospike

import scala.concurrent.Future
import com.aerospike.client.Key

/**
 * Operations on an Aerospike namespace.
 *
 * @tparam K the key type
 * @tparam V the value type. If you have bins / sets with different types, use AnyRef and cast.
 */
trait Namespace[K, V] {
  /**
   * Gets the default bin for a single key.
   */
  def get(key: K, set: String = "", bin: String = ""): Future[Option[V]]

  /**
   * Gets multiple bins for a single key.
   */
  def getBins(key: K, set: String = "", bins: Seq[String]) : Future[Map[String, V]]

  /**
   * Gets the default bin for multiple keys.
   */
  def multiGet(keys: Seq[K], set: String = "", bin: String = ""): Future[Map[K, Option[V]]]

  /**
   * Gets multiple bins for a single key.
   */
  def multiGetBins(keys: Seq[K], set: String = "", bins: Seq[String]): Future[Map[K, Map[String, V]]]

  /**
   * Put a value into a key.
   */
  def put(key: K, value: V, set: String = "", bin: String = "", customTtl: Option[Int] = None): Future[Unit]

  /**
   * Delete a key.
   */
  def delete(key: K, set: String = "", bin: String = "") : Future[Unit]
}

