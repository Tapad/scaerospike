## Intro

Scaerospike is a Scala wrapper around the Aerospike 3.x series asynchronous Java client. It uses the Scala 2.10 Futures
API.

## Getting started

```scala
    import com.tapad.aerospike._
    import scala.concurrent.Future
    import scala.concurrent.duration._
    import scala.util.{Failure, Success}
    import scala.concurrent.ExecutionContext.Implicits.global

    val client = AerospikeClient(Seq("192.168.210.129"))

    val devices = client.namespace("devices").set[String, String]("setName")

    val write : Future[Unit] 	          = devices.put("deviceA", "foo")
    val read  : Future[Option[String]]    = devices.get("deviceA")

    val result = write flatMap (_ => read)

    result.onComplete {
      case Failure(ex) 		=> ex.printStackTrace()
      case Success(value) 	=> println("Successfully wrote and read %s".format(value))
    }

    Await.result(result, 10 seconds)
```   	
