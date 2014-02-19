package com.tapad.aerospike

import com.aerospike.client.async.{MaxCommandAction, AsyncClientPolicy}
import com.aerospike.client.policy.{WritePolicy, QueryPolicy}

/**
 * Aerospike client settings.
 *
 * @param maxCommandsOutstanding the maximum number of outstanding commands before rejections will happen
 */
case class ClientSettings(maxCommandsOutstanding: Int = 500, selectorThreads: Int = 1, maxSocketIdle : Int = 14) {

  /**
   * @return a mutable policy object for the Java client.
   */
  private[aerospike] def buildClientPolicy() = {
    val p = new AsyncClientPolicy()
    p.asyncMaxCommandAction = MaxCommandAction.REJECT
    p.asyncMaxCommands      = maxCommandsOutstanding
    p.asyncSelectorThreads  = selectorThreads
    p.maxSocketIdle         = maxSocketIdle
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

