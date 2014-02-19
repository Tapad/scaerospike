package com.tapad.aerospike

import scala.concurrent.Await
import com.aerospike.client.Log
import com.aerospike.client.Log.{Level, Callback}


object AerospikeClientSpec {
  def main(args: Array[String]) {


    import com.tapad.aerospike._
    import scala.concurrent.Future
    import scala.concurrent.duration._
    import scala.util.{Failure, Success}
    import scala.concurrent.ExecutionContext.Implicits.global

    com.aerospike.client.Log.setLevel(Log.Level.DEBUG)
    com.aerospike.client.Log.setCallback(new Callback {
      override def log(level: Level, message: String): Unit = {
        println(level + ": " + message)
      }
    })
    val client = AerospikeClient(Seq("192.168.210.129"))

    val devices = client.namespace("test").defaultSet[String, String]


    def log[T](f: scala.concurrent.Future[T]) : Unit = f.andThen { case x => println(x) }

//    log(all.put("key2", value = "key2Value"))

//    log(devices.multiGet(Seq("key", "key2")))
    log(devices.multiGetBins(Seq("key", "key2"), bins = Seq("", "bin")))
//    log(devices.getBins("key", bins = Seq("","bin")))
//    log(all.getBins("key", bins = Seq("bin")))

    Thread.sleep(1000000)



  }
}
