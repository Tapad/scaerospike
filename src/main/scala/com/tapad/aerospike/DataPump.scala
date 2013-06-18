package com.tapad.aerospike

import com.aerospike.client.async.{AsyncClientPolicy, MaxCommandAction, AsyncClient}
import java.util.concurrent.atomic.AtomicInteger
import com.aerospike.client.policy.{ClientPolicy, WritePolicy, ScanPolicy}
import com.aerospike.client.{Bin, Record, Key, ScanCallback}
import java.util
import scala.collection.JavaConverters._
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


    val destination = {
      val clientPolicy = new ClientPolicy
      new com.aerospike.client.AerospikeClient(clientPolicy, destAddr ,3000)
    }

    println("Copying all data from namespace %s from cluster at %s to %s...".format(namespace, sourceAddr, destAddr))

    val recordsMoved = new AtomicInteger()
    val errors = new AtomicInteger()

    val scanPolicy = new ScanPolicy()

    val writePolicy = new WritePolicy()
    writePolicy.timeout = 100000
    var startTime = System.currentTimeMillis()

    val batchSize = 100000

    source.scanAll(scanPolicy, namespace, set, new ScanCallback {
      def scanCallback(key: Key, record: Record) {
        val bins = new util.ArrayList[Bin]()
        val i = record.bins.entrySet().iterator()
        while (i.hasNext) {
          val e = i.next()
          bins.add(new Bin(e.getKey, e.getValue))
        }
        try {
          destination.put(writePolicy, key, bins.asScala: _*)
        } catch {
          case e : Exception => errors.incrementAndGet()
        }
        val count = recordsMoved.incrementAndGet()
        if (count % 100000 == 0) {
          val elapsed = System.currentTimeMillis() - startTime
          startTime = System.currentTimeMillis()
          println("Processed %(,d records, %(,d errors, %d ms, %.2f records / sec".format(
            count, errors.get(), elapsed, batchSize.toFloat / elapsed * 1000)
          )
        }
      }
    })
    println("Done, a total of %d records moved...".format(recordsMoved.get()))
  }
}
