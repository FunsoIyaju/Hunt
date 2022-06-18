import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountSessionStreamApp {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()

    val context = spark.sparkContext

    val streamContext = new StreamingContext(context, Seconds(40))
    streamContext.checkpoint("D:\\Checkpoint")

    val lines = streamContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordcount = pairs.reduceByKey(_ + _)
    wordcount.print()
    streamContext.start()
    streamContext.awaitTermination()
  }

}
