package com.tapad.aerospike

import com.aerospike.client.async.{AsyncClientPolicy, AsyncClient}
import java.util.concurrent.atomic.AtomicInteger
import com.aerospike.client.policy.{WritePolicy, ScanPolicy}
import com.aerospike.client.{Bin, Record, Key, ScanCallback}
import scala.concurrent.duration._
import java.util
import scala.collection.JavaConverters._
import java.util.concurrent.{TimeUnit, LinkedBlockingDeque, Executors}
import scala.concurrent.{Await, Future}

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
      clientPolicy.maxSocketIdle = 3600
      new AsyncClient(clientPolicy, destAddr ,3000)
    }

    println("Copying all data from namespace %s from cluster at %s to %s...".format(namespace, sourceAddr, destAddr))

    val written = new ProgressWriter("Writes")
    val reads = new ProgressWriter("Reads")
    val errors = new ProgressWriter("Errors", 100)

    val scanPolicy = new ScanPolicy()
    val writePolicy = new WritePolicy()
    writePolicy.maxRetries = 0

    val WriterCount = 32
    implicit val executor = scala.concurrent.ExecutionContext.fromExecutor(Executors.newFixedThreadPool(WriterCount))
    val workQueue = new LinkedBlockingDeque[(Key, util.ArrayList[Bin])](2000)

    var finished = false

    val ops = for { i <- 0 to WriterCount} yield {
      Future {
        while (!finished) {
          workQueue.poll(5, TimeUnit.SECONDS) match {
            case (key, bins) =>
              try {
                destination.put(writePolicy, key, bins.asScala: _*)
                written.progress()
              } catch {
                case e : Exception => errors.progress()
              }
            case null => // No work, check if done
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
    println("Finished. Waiting for threads...")
    finished = true
    Await.result(Future.sequence(ops), 5000 hours)
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
