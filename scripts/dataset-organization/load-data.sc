import scala.sys.env
import org.apache.spark.sql.functions._

spark.conf.set("spark.sql.parquet.mergeSchema", "true")
spark.conf.set("spark.sql.parquet.schemaVerticalMergeMode", "vertical_relaxed")

val pgHost = env("POSTGRES_HOST")
val pgPort = env("POSTGRES_PORT")
val pgUser = env("POSTGRES_USERNAME")
val pgPassword = env("POSTGRES_PASSWORD")
val pgDatabase = env("POSTGRES_DATABASE")
val pgTable = "green_tripdata"

val jdbcUrl = s"jdbc:postgresql://$pgHost:$pgPort/$pgDatabase"

spark.read
  .parquet("data/*.parquet")
  .withColumn("filename", input_file_name())
  .withColumn("date", regexp_extract(col("filename"), ".*/green_tripdata_([\\d]{4}-[\\d]{2}).*\\.parquet", 1))
  .withColumn("date", to_date(col("date"), "yyyy-MM"))
  .withColumn("year", year(col("date")))
  .withColumn("month", month(col("date")))
  .drop(col("date"), col("filename"))
  .repartition(col("year"), col("month"))
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", pgTable)
  .option("user", pgUser)
  .option("password", pgPassword)
  .option("driver", "org.postgresql.Driver")
  .option("batchsize", 10000)
  .mode("overwrite")
  .save()
