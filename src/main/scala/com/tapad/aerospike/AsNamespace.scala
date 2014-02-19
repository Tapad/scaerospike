package com.tapad.aerospike

import scala.concurrent.ExecutionContext


/**
 * Represents a namespace tied to a specific client.
 *
 * @param client the client to use
 * @param name the name of the namespace
 * @param readSettings settings for reads
 * @param writeSettings settings for writes
 */
class AsNamespace(private final val client: AerospikeClient,
                  name: String,
                  readSettings: ReadSettings,
                  writeSettings: WriteSettings) {

  def set[K, V](setName: String,
                readSettings: ReadSettings = readSettings,
                writeSettings: WriteSettings = writeSettings)
               (implicit keyGen: KeyGenerator[K], executionContext: ExecutionContext): AsSetOps[K, V] =
    new AsSet[K, V](client, name, setName, readSettings, writeSettings)

  def defaultSet[K, V](implicit keyGen: KeyGenerator[K], executionContext: ExecutionContext): AsSetOps[K, V] =
    new AsSet[K, V](client, name, "", readSettings, writeSettings)
}

