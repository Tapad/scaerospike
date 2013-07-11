package com.tapad.aerospike

import scala.concurrent.Await

object AerospikeClientSpec {
  def main(args: Array[String]) {


    import com.tapad.aerospike._
    import scala.concurrent.Future
    import scala.concurrent.duration._
    import scala.util.{Failure, Success}
    import scala.concurrent.ExecutionContext.Implicits.global

    val client = AerospikeClient(Seq("172.16.0.100"))

    val devices = client.namespace[String, String]("devices")


    val sets = Seq("pd678", "pd652", "pd630", "pd2")
    Future.sequence(sets.map(s => devices.get("3946c5d8-c262-4997-a9df-b6e0914374b1", set = s).map(s -> _))).onComplete {
      case Failure(ex) 		=> ex.printStackTrace()
      case Success(value) 	=> println("Successfully wrote and read %s".format(value))
    }

//    val write : Future[Unit] 	          = devices.put("set1", "set1", set = "set1").map(_ => devices.put("set2", "set2", set = "set2"))
    val read  : Future[Option[String]]  = devices.get("OH0ca069c1-ca1c-11e1-a5fc-12313b033616", set = "pd678")

    val result = read
//    val result = write flatMap (_ => read)

    result.onComplete {
      case Failure(ex) 		=> ex.printStackTrace()
      case Success(value) 	=> println("Successfully wrote and read %s".format(value))
    }

    Await.result(result, 10 seconds)
//    import com.tapad.aerospike._
//    import scala.concurrent.Future
//    import scala.concurrent.duration._
//    import scala.util.{Failure, Success}
//    import scala.concurrent.ExecutionContext.Implicits.global
//
//    val client = AerospikeClient(Seq("192.168.0.18"))
//
//    Thread.sleep(1000)
//
//    val devices = client.namespace[String, String]("devices")
//
//    val id = "a"
//    val execution = devices.get(id) map { value =>
//      val newValue = (value.getOrElse("1").toInt + 1).toString
//      devices.put(id, newValue)
//      newValue
//    }
//
//    println(Await.result(execution, 1 second))

//    devices.put("deviceA", "foo") onComplete {
//      case Failure(ex) => ex.printStackTrace()
//      case Success(_) => println("Successfully wrote")
//    }
//
//    devices.get("foo") onComplete {
//      case Failure(ex) => ex.printStackTrace()
//      case Success(value) => println("Successfully wrote and read %s".format(value))
//    }
//
//    val write: Future[Unit] = devices.put("deviceA", "foo")
//    val read: Future[Option[String]] = devices.get("deviceA")
//
//    write.flatMap(_ => read) onComplete {
//      case Failure(ex) 		=> ex.printStackTrace()
//      case Success(value) 	=> println("Successfully wrote and read %s".format(value))
//    }

  }
}
