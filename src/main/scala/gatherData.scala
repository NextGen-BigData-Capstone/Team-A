import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.ProcessingTime

object DummyProject3{
    def main(args:Array[String]): Unit = {
        val spark = SparkSession
            .builder
            .appName("DummyProject3")
            .master("local")
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        val df = spark
            .readStream
            .option("startingOffsets", "earliest")
            .format("kafka")
            .option("kafka.bootstrap.servers","52.91.98.39:9092")
            .option("subscribe", "Gabriel-BigData")
            .load()
        /*df.writeStream
            .format("console")
            .start()*/
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").repartition(1)
            .writeStream
            //.format(kafka)
            .trigger(ProcessingTime("1800 seconds"))
            .format("csv")
            .option("format", "append")
            .option("path", "Data")
            .outputMode("append")
            .option("checkpointLocation", "checkpoint/")
            .option("truncate", "false")
            .start()
            .awaitTermination()
        spark.close
    }
}