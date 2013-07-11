package com.tapad.aerospike

import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import scala.concurrent.{Promise, Await}
import scala.annotation.tailrec

object Benchmark {
  def main(args: Array[String]) {
    import com.tapad.aerospike._
    import scala.concurrent.Future
    import scala.concurrent.duration._
    import scala.util.{Failure, Success}
    import scala.concurrent.ExecutionContext.Implicits.global

    val maxOutstanding  = 1000
    val concurrency     = 100
    val totalRequests   = 1000000

    val client = AerospikeClient(
      Seq("192.168.0.18"),
      ClientSettings(maxCommandsOutstanding = maxOutstanding)
    ).namespace[String, Array[Byte]]("devices")


    val key = new AtomicInteger(0)
    val submitted = new AtomicInteger(0)
    val successes = new AtomicInteger(0)
    val completedRequests = new AtomicInteger()
    val errors = new AtomicInteger(0)
    val bytesReceived = new AtomicLong(0)


    val start = System.currentTimeMillis()

    val blanks = Array.ofDim[Byte](500)

    def executeInBatch(count: Int, batchSize: Int): Future[_] = {
      val thisBatch = math.min(count, batchSize)
//      println("%d to go, %d in this batch".format(count, thisBatch))
      val batch = (0 until thisBatch).map { _ =>
        val k = key.incrementAndGet().toString
//                client.put(k, blanks)
        val f = client.get(key = k)
        submitted.incrementAndGet()

        f.onComplete {
          case Success(v) =>
            v.map(_.length).foreach(c => bytesReceived.addAndGet(c))
            successes.incrementAndGet()
            completedRequests.incrementAndGet()
          case Failure(_) =>
            errors.incrementAndGet()
            completedRequests.incrementAndGet()
        }
        f.recoverWith{ case _ => Future { Unit } }
      }
      Future.sequence(batch).flatMap { _ =>
        val remaining = count - thisBatch
        if (remaining > 0) {
          executeInBatch(remaining, batchSize)
        }
        else {
          Promise.successful(Unit).future
        }
      }
    }


    val requests = (0 until concurrency).map { _ =>
      executeInBatch(totalRequests / concurrency, maxOutstanding / concurrency)
    }

    println("waiting1")
    val all = Future.sequence(requests)

    println("waiting")
    all.onComplete {
      case Success(s) => println("Success")
      case Failure(e) => e.printStackTrace()
    }
    println(Await.ready(all, 10 minutes))


    val duration = System.currentTimeMillis() - start
    println("%20s%20s\t%s\t%s".format("Submitted", "Compl", "Succ", "Err"))
    println("%20s%20s\t%d\t%d".format(submitted.get(), completedRequests.get(), successes.get(), errors.get()))
    println("================")
    println("%d requests completed in %dms (%f requests per second)".format(
      completedRequests.get, duration,
      totalRequests.toFloat / duration.toFloat * 1000))
    println("%d errors".format(errors.get))
    println("%2f MBytes received".format(bytesReceived.get() / 1000000.0))

    System.exit(0)
  }

}
