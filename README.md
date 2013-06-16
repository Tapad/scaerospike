# Getting started


    import com.tapad.aerospike._
    import scala.concurrent.Future
    import scala.concurrent.duration._
    import scala.util.{Failure, Success}
    import scala.concurrent.ExecutionContext.Implicits.global

    val client = AerospikeClient(Seq("192.168.0.18"))

    val devices = client.namespace[String, String]("devices")

    val write : Future[Unit] 	          = devices.put("deviceA", "foo")
    val read  : Future[Option[String]]  = devices.get("deviceA")

    val result = write flatMap (_ => read)

    result.onComplete {
      case Failure(ex) 		=> ex.printStackTrace()
      case Success(value) 	=> println("Successfully wrote and read %s".format(value))
    }

    Await.result(result, 10 seconds)
   	
