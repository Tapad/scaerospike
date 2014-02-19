package com.tapad.aerospike

import com.aerospike.client.Key

/**
 * The AS client no longer supports AnyRef keys, we need a way to create keys from values.
 * We define this as a separate case class wrapping the generator function in order to define
 * the implicit implementations in the companion.
 */
case class KeyGenerator[K](gen: (String, String, K) => Key) {
  final def apply(ns: String, set: String, key: K) : Key = gen(ns, set, key)
}

object KeyGenerator {
  implicit val StringKeyGenerator     = KeyGenerator((ns: String, set: String, key: String) => new Key(ns, set, key))
  implicit val IntKeyGenerator        = KeyGenerator((ns: String, set: String, key: Int) => new Key(ns, set, key))
  implicit val ByteArrayKeyGenerator  = KeyGenerator((ns: String, set: String, key: Array[Byte]) => new Key(ns, set, key))
  implicit val LongKeyGenerator       = KeyGenerator((ns: String, set: String, key: Long) => new Key(ns, set, key))
}
