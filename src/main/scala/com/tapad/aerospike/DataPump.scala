package com.tapad.aerospike

import com.aerospike.client.async.{AsyncClientPolicy, MaxCommandAction, AsyncClient}
import java.util.concurrent.atomic.AtomicInteger
import com.aerospike.client.policy.{ClientPolicy, WritePolicy, ScanPolicy}
import com.aerospike.client.{Bin, Record, Key, ScanCallback}
import java.util
import scala.collection.JavaConverters._
import com.aerospike.client.listener.WriteListener
import java.util.concurrent.{LinkedBlockingDeque, Executors}
import scala.concurrent.Future

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
      val clientPolicy = new AsyncClientPolicy
      new AsyncClient(clientPolicy, destAddr ,3000)
    }

    println("Copying all data from namespace %s from cluster at %s to %s...".format(namespace, sourceAddr, destAddr))

    val written = new ProgressWriter("Writes")
    val reads = new ProgressWriter("Reads")
    val errors = new ProgressWriter("Errors")

    val scanPolicy = new ScanPolicy()
    val writePolicy = new WritePolicy()

    val WriterCount = 64
    implicit val executor = scala.concurrent.ExecutionContext.fromExecutor(Executors.newFixedThreadPool(WriterCount))
    val workQueue = new LinkedBlockingDeque[(Key, util.ArrayList[Bin])](20000)

    for { i <- 0 to WriterCount} {
      Future {
        while (true) {
          val (key, bins) = workQueue.poll()
          try {
            destination.put(writePolicy, key, bins.asScala: _*)
            written.progress()
          } catch {
            case e : Exception => errors.progress()
          }
        }
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
        workQueue.put(key -> bins)
        reads.progress()
      }
    })
    println("Done, a total of %d records moved...".format(written.get()))
  }

  class ProgressWriter(operation: String) {
    var startTime = System.currentTimeMillis()
    val ops = new AtomicInteger()
    val batchSize = 10000

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
