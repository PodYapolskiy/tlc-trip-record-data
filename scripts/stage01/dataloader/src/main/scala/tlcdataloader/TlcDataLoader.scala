package tlcdataloader

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration._
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import scala.util.{Try, Success, Failure}

object TlcDataLoader extends App {
  val argMap = parseArgs(args)

  val pgHost        = argMap.getOrElse("host", throw new IllegalArgumentException("--host is required"))
  val pgPort        = argMap.getOrElse("port", throw new IllegalArgumentException("--port is required"))
  val pgUser        = argMap.getOrElse("username", throw new IllegalArgumentException("--username is required"))
  val pgPassword    = argMap.getOrElse("password", throw new IllegalArgumentException("--password is required"))
  val pgDatabase    = argMap.getOrElse("database", throw new IllegalArgumentException("--database is required"))
  val pgTable       = argMap.getOrElse("table", throw new IllegalArgumentException("--table is required"))
  val parquetSource = argMap.getOrElse("source", throw new IllegalArgumentException("--source is required"))
  val parquetMerged = argMap.getOrElse("merged", throw new IllegalArgumentException("--merged is required"))

  val spark = SparkSession
    .builder()
    .appName("TLC Trip Record Data Loader")
    .getOrCreate()

  import spark.implicits._
  import scala.concurrent.ExecutionContext.Implicits.global

  val jdbcUrl    = s"jdbc:postgresql://$pgHost:$pgPort/$pgDatabase"
  val hadoopConf = spark.sparkContext.hadoopConfiguration
  val fs         = FileSystem.get(hadoopConf)

  val sourcePath = new Path(parquetSource)
  val parquetFiles = fs
    .listStatus(sourcePath)
    .filter(file => file.getPath.getName.matches("green_tripdata_\\d{4}-\\d{2}\\.parquet"))
    .map(_.getPath.toString)
    .toList

  val processingTasks = parquetFiles.map { filePath =>
    Future {
      val fileName = new Path(filePath).getName
      println(s"Processing file: $fileName")

      val regex = "green_tripdata_(\\d{4})-(\\d{2})\\.parquet".r
      val (year, month) = fileName match {
        case regex(y, m) => (y.toInt, m.toInt)
        case _ =>
          throw new IllegalArgumentException(
            s"Invalid filename format: $fileName"
          )
      }

      spark.read
        .parquet(filePath)
        .withColumn("VendorID", col("VendorID").cast(LongType))
        .withColumn("lpep_pickup_datetime", col("lpep_pickup_datetime").cast(TimestampType))
        .withColumn("lpep_dropoff_datetime", col("lpep_dropoff_datetime").cast(TimestampType))
        .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(StringType))
        .withColumn("RatecodeID", col("RatecodeID").cast(LongType))
        .withColumn("PULocationID", col("PULocationID").cast(LongType))
        .withColumn("DOLocationID", col("DOLocationID").cast(LongType))
        .withColumn("passenger_count", col("passenger_count").cast(LongType))
        .withColumn("trip_distance", col("trip_distance").cast(DoubleType))
        .withColumn("fare_amount", col("fare_amount").cast(DoubleType))
        .withColumn("extra", col("extra").cast(DoubleType))
        .withColumn("mta_tax", col("mta_tax").cast(DoubleType))
        .withColumn("tip_amount", col("tip_amount").cast(DoubleType))
        .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType))
        .withColumn("ehail_fee", col("ehail_fee").cast(DoubleType))
        .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType))
        .withColumn("total_amount", col("total_amount").cast(DoubleType))
        .withColumn("payment_type", col("payment_type").cast(LongType))
        .withColumn("trip_type", col("trip_type").cast(DoubleType))
        .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType))
        .withColumn("year", lit(year))
        .withColumn("month", lit(month))
        .write
        .mode("overwrite")
        .parquet("%s/year=%04d/month=%02d".format(parquetMerged, year, month))

      (year, month)
    }
  }

  Await.result(Future.sequence(processingTasks), 1.hour)

  spark.read
    .parquet(parquetMerged)
    .repartition(col("year"), col("month"))
    .write
    .mode("overwrite")
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", pgTable)
    .option("user", pgUser)
    .option("password", pgPassword)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", 10000)
    .save()

  spark.stop()

  def parseArgs(args: Array[String]): Map[String, String] = {
    val argList = args.toList

    def nextArg(
        list: List[String],
        map: Map[String, String]
    ): Map[String, String] = {
      list match {
        case Nil => map
        case arg :: value :: rest if arg.startsWith("--") =>
          val key = arg.substring(2)
          nextArg(rest, map + (key -> value))
        case _ :: rest => nextArg(rest, map)
      }
    }

    nextArg(argList, Map())
  }
}
