package streaming

object SparkStreaming {

  import org.apache.spark.SparkConf
  import org.apache.spark.streaming.{Seconds, StreamingContext}
  import org.apache.spark.storage.StorageLevel
  import org.apache.spark.streaming.receiver.Receiver
  import scala.util.Random

  def main ( args: Array[String] ): Unit = {
    val sc = new SparkConf ().setAppName ( "Spark streaming example" ).setMaster ( "local[*]" )
    val batchInterval = Seconds ( 5 )
    val threshold3 = 100
    val ssc = new StreamingContext ( sc, batchInterval )
    ssc.sparkContext.setLogLevel ( "ERROR" )
    import org.apache.spark.storage.StorageLevel._

    //val nc1 = ssc.socketTextStream("localhost", 9960) //started with nc -l -p 9960 localhost
    val hdfs1 = ssc.textFileStream("hdfs://stream:9999/")
    val s1 = ssc.receiverStream ( new InfiniteStreamReceiver ( "a", Stream.from ( 100 ), 500, storageLevel = MEMORY_ONLY ) )
    val s2 = ssc.receiverStream ( new InfiniteStreamReceiver ( "b", Stream.from ( 100 ), 200, storageLevel = MEMORY_ONLY ) )
    val s3 = ssc.receiverStream ( new InfiniteStreamReceiver ( "c", Stream.from ( 300 ), 300, storageLevel = MEMORY_ONLY ) )

    //do this actions in loop for each new data incoming in batch interval
    s1.window(Seconds(20)).join(s2.window(Seconds(30))).map(v => s"join ${v._1} - ${v._2}").print
    s3.window(Seconds(20), slideDuration = Seconds(10))
      .union(s2.window(Seconds(30), slideDuration = Seconds(10))).filter(_._1 % threshold3 > 1).map(_._2).reduce(_ + _).print
    hdfs1.map ( x => s"hdfs: $x" ).print //don' monitor directory :(

    //start context
    ssc.start
    ssc.awaitTermination
  }

  class InfiniteStreamReceiver[T] ( name: String, stream: Stream[T], delay: Int = 0, storageLevel: StorageLevel ) extends Receiver[(T, Int)]( storageLevel ) {

    override def onStart ( ): Unit = {
      new Thread ( s"InfiniteStreamReceiver-${name}" ) {
        override def run ( ): Unit = {
          stream.takeWhile { _ => Thread.sleep ( delay ); !isStopped }.foreach ( x => store( (x, new Random().nextInt)) )
        }

        setDaemon ( true )
      }.start ()
    }

    override def onStop ( ): Unit = {}
  }

}
