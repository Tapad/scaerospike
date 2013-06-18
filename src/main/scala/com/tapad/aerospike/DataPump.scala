package com.tapad.aerospike

import com.aerospike.client.async.{MaxCommandAction, AsyncClientPolicy, AsyncClient}
import java.util.concurrent.atomic.AtomicInteger
import com.aerospike.client.policy.{ClientPolicy, WritePolicy, ScanPolicy}
import com.aerospike.client._
import scala.concurrent.duration._
import java.util
import scala.collection.JavaConverters._
import java.util.concurrent.{TimeUnit, LinkedBlockingDeque, Executors}
import scala.concurrent.{Await, Future}
import com.aerospike.client.Log.{Level, Callback}
import com.aerospike.client.listener.WriteListener

object DataPump {
  def main(args: Array[String]) {

    val sourceAddr = args(0)
    val destAddr = args(1)
    val namespace = args(2)
    val set = if (args.size == 4) args(3) else ""


    val source = {
      val clientPolicy = new AsyncClientPolicy
      new AsyncClient(clientPolicy, sourceAddr, 3000)
    }

    Log.setLevel(Log.Level.DEBUG)
    Log.setCallback(new Callback {
      def log(level: Level, message: String) {
        println(level + ": " + message)
      }
    })


    val destination = {
      val clientPolicy = new AsyncClientPolicy
      clientPolicy.asyncMaxCommandAction = MaxCommandAction.BLOCK
      clientPolicy.maxSocketIdle = 3600
      clientPolicy.timeout = 1000
      clientPolicy.maxThreads = 10
      new AsyncClient(clientPolicy, destAddr, 3000)
    }

    println("Copying all data from namespace %s from cluster at %s to %s...".format(namespace, sourceAddr, destAddr))

    val written = new ProgressWriter("Writes")
    val reads = new ProgressWriter("Reads")
    val errors = new ProgressWriter("Errors", 100)

    val scanPolicy = new ScanPolicy()
    val writePolicy = new WritePolicy()
    writePolicy.maxRetries = 0
    writePolicy.sleepBetweenRetries = 50

    val writeListener = new WriteListener {
      def onFailure(exception: AerospikeException) {
        errors.progress()
      }

      def onSuccess(key: Key) {
        written.progress()
      }
    }

    source.scanAll(scanPolicy, namespace, set, new ScanCallback {
      def scanCallback(key: Key, record: Record) {
        val bins = new util.ArrayList[Bin]()
        val i = record.bins.entrySet().iterator()
        while (i.hasNext) {
          val e = i.next()
          bins.add(new Bin(e.getKey, e.getValue))
        }
        reads.progress()
        destination.put(writePolicy, writeListener, key, bins.asScala : _*)
      }
    })
    println("Finished. Waiting for threads...")
    println("Done, a total of %d records moved...".format(written.get()))
  }

  class ProgressWriter(operation: String, batchSize : Int = 10000) {
    var startTime = System.currentTimeMillis()
    val ops = new AtomicInteger()

    def get() = ops.get()

    def progress() {
      val count = ops.incrementAndGet()
      if (count % batchSize == 0) {
        val elapsed = System.currentTimeMillis() - startTime
        startTime = System.currentTimeMillis()
        println("%s: %(,d records, %d ms, %.2f records / sec".format(
          operation,
          count, elapsed, batchSize.toFloat / elapsed * 1000)
        )
      }

    }
  }
}
