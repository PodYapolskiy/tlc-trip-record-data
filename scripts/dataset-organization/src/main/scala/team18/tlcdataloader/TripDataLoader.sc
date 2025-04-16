package team18.tlcdataloader

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

class TripDataLoader {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TLC Trip Record Data Loader")
      .getOrCreate()

    spark.conf.set("spark.sql.parquet.mergeSchema", "true")
    spark.conf.set("spark.sql.parquet.schemaVerticalMergeMode", "vertical_relaxed")

    val pgHost = sys.env("POSTGRES_HOST")
    val pgPort = sys.env("POSTGRES_PORT")
    val pgUser = sys.env("POSTGRES_USERNAME")
    val pgPassword = sys.env("POSTGRES_PASSWORD")
    val pgDatabase = sys.env("POSTGRES_DATABASE")
    val pgTable = "green_tripdata"

    val jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDatabase"

    spark.read
      .parquet(sys.env("PARQUET_SOURCE"))
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
      .option("batchsize", 10000)
      .mode("overwrite")
      .save()

    spark.stop()
  }
}
