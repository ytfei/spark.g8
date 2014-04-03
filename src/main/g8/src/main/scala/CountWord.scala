import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._

/**
 * This only a sample
 */
class CountWord {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: CountWord <master> <host> <port>")
      System.exit(1)
    }

    // Create the context
    val ssc = new StreamingContext(args(0), "CountWord", Seconds(2),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.socketTextStream(args(1), args(2).toInt, StorageLevel.MEMORY_AND_DISK)

    val words = lines.map(l => {
      val c = System.currentTimeMillis()
      println("mapping " + c)
      s"\$c \$l"
    }).flatMap(l => {
      val x = l.substring(1, 10)
      println(s"flatmapping \$x")
      l.split(" ")
    })

    // val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    val wordCounts = words.reduce((a, b) => (a.length + b.length).toString)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

    //    val sc = new SparkContext(args(0), "CountWord",
    //      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    //    val source = sc.makeRDD(1 to 10000, 10)
    //    val r = source.map(i => {
    //      println("mapping " + i)
    //      i + 2
    //    }).reduce((a, b) => {
    //      println(s"reducing \$a and \$b")
    //      a + b
    //    })
    //
    //    println("Finally, we got: " + r)
  }
}
