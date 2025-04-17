package tlcdataloader

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object TlcDataLoader extends App {
  val argMap = parseArgs(args)

  val pgHost = argMap.getOrElse(
    "host",
    throw new IllegalArgumentException("--host is required")
  )
  val pgPort = argMap.getOrElse(
    "port",
    throw new IllegalArgumentException("--port is required")
  )
  val pgUser = argMap.getOrElse(
    "username",
    throw new IllegalArgumentException("--username is required")
  )
  val pgPassword = argMap.getOrElse(
    "password",
    throw new IllegalArgumentException("--password is required")
  )
  val pgDatabase = argMap.getOrElse(
    "database",
    throw new IllegalArgumentException("--database is required")
  )
  val pgTable = argMap.getOrElse("table", "green_tripdata")
  val parquetSource = argMap.getOrElse(
    "source",
    throw new IllegalArgumentException("--source is required")
  )

  val spark = SparkSession
    .builder()
    .appName("TLC Trip Record Data Loader")
    .getOrCreate()

  val schema = StructType(
    Array(
      StructField("VendorID", LongType, true),
      StructField("lpep_pickup_datetime", TimestampType, true),
      StructField("lpep_dropoff_datetime", TimestampType, true),
      StructField("store_and_fwd_flag", StringType, true),
      StructField("RatecodeID", LongType, true),
      StructField("PULocationID", LongType, true),
      StructField("DOLocationID", LongType, true),
      StructField("passenger_count", LongType, true),
      StructField("trip_distance", DoubleType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("extra", DoubleType, true),
      StructField("mta_tax", DoubleType, true),
      StructField("tip_amount", DoubleType, true),
      StructField("tolls_amount", DoubleType, true),
      StructField("ehail_fee", DoubleType, true),
      StructField("improvement_surcharge", DoubleType, true),
      StructField("total_amount", DoubleType, true),
      StructField("payment_type", LongType, true),
      StructField("trip_type", DoubleType, true),
      StructField("congestion_surcharge", DoubleType, true),
      StructField("year", LongType, true),
      StructField("month", LongType, true)
    )
  )

  spark.conf.set("spark.sql.parquet.mergeSchema", "true")
  spark.conf.set(
    "spark.sql.parquet.schemaVerticalMergeMode",
    "vertical_relaxed"
  )

  val jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDatabase"

  spark.read
    .option("mergeSchema", "false")
    .schema(schema)
    .parquet(parquetSource)
    .withColumn("filename", input_file_name())
    .withColumn(
      "date",
      regexp_extract(
        col("filename"),
        "green_tripdata_([\\d]{4}-[\\d]{2}).*\\.parquet",
        1
      )
    )
    .withColumn("date", to_date(col("date"), "yyyy-MM"))
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .drop("date")
    .drop("filename")
    .repartition(col("year"), col("month"))
    .write
    .format("jdbc")
    .option("url", jdbcUrl)
    .option("dbtable", pgTable)
    .option("user", pgUser)
    .option("password", pgPassword)
    .option("driver", "org.postgresql.Driver")
    .option("batchsize", 5000)
    .mode("overwrite")
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
