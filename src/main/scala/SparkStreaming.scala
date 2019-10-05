import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

object SparkStreaming {

  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  def main ( args: Array[String] ): Unit = {
    import org.apache.log4j.{Level, Logger}
    val log = Logger.getLogger ( getClass.getName )

    val sc = new SparkConf ().setAppName ( "Spark streaming example" ).setMaster ( "local[*]" )
    val batchInterval = Seconds ( 1 )
    val threshold3 = 100
    val ssc = new StreamingContext ( sc, batchInterval )
    ssc.sparkContext.setLogLevel("ERROR")
    //define 4 streaming
    import org.apache.spark.storage.StorageLevel._
    //ssc.socketTextStream("localhost", 9999) //started with nc -l -p 9999 localhost
    val s1 = ssc.receiverStream(new InfiniteStreamReceiver(Stream.from(-100), 500, storageLevel = MEMORY_ONLY))
    val s2 = ssc.receiverStream(new InfiniteStreamReceiver(Stream.from(100), 500, storageLevel = MEMORY_ONLY))
    val s3 = ssc.receiverStream ( new InfiniteStreamReceiver ( Stream from(100), 200, storageLevel = MEMORY_ONLY ) )
    val s4 = ssc.receiverStream(new InfiniteStreamReceiver(Stream.from(100), 220, storageLevel = MEMORY_ONLY))

    //do this actions in loop for each new data incoming in batch interval
    s1.union(s2).map(v => s"Value of one of first two stream $v").print

    //s3.window(batchInterval * 3, batchInterval * 2 ).foreachRDD((rdd, t) => log.info("# " + (t.milliseconds, rdd.mean)))
     // .filter ( _ > threshold3 ).count

    s4.map(x => s"Sensor4: $x").print

    //start context
    ssc.start ()
    ssc.awaitTermination ()
  }

  class InfiniteStreamReceiver[T] ( stream: Stream[T], delay: Int = 0, storageLevel: StorageLevel ) extends Receiver[T]( storageLevel ) {

    override def onStart ( ): Unit = {
      new Thread ( "InfiniteStreamReceiver" ) {
        override def run ( ): Unit = {
          stream.takeWhile { _ => Thread.sleep ( delay ); !isStopped }.foreach ( store )
        }
        setDaemon ( true )
      }.start ()
    }

    override def onStop ( ): Unit = {}
  }

}
