import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf



# TODO Create a schema for incoming resources
schema = StructType([
        StructField("crime_id", StringType(), True),
        StructField("original_crime_type_name", StringType(), False),
        StructField("report_date", StringType(), False),
        StructField("call_date", StringType(), False),
        StructField("offense_date", StringType(), False),
        StructField("call_time", StringType(), False),
        StructField("call_date_time", StringType(), False),
        StructField("disposition", StringType(), False),
        StructField("address", StringType(), False),
        StructField("city", StringType(), False),
        StructField("state", StringType(), False),
        StructField("agency_id", StringType(), True),
        StructField("address_type", StringType(), False),
        StructField("common_location", StringType(), False)
])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format('kafka') \
        .option("startingOffsets", "earliest") \
        .option('kafka.bootstrap.servers', 'localhost:9092') \
        .option('subscribe', 'com.udacity.police_calls') \
        .option('maxRatePerPartition', 100) \
        .option('maxOffsetPerTrigger', 200) \
        .load()
        



    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS String)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(psf.col("original_crime_type_name"),
            psf.col("disposition"))
    distinct_table = service_table \
        .select(
        psf.to_timestamp(psf.col("call_date_time")).alias("call_date_time"),
        psf.col("original_crime_type_name"),
        psf.col("disposition")
    )
    distinct_table.printSchema()

    # count the number of original crime type
    #agg_df = distinct_table.groupBy("original_crime_type_name").count()
    agg_df = distinct_table \
        .select(
        distinct_table.call_date_time,
        distinct_table.original_crime_type_name,
        distinct_table.disposition
    ) .groupBy(
        psf.window(distinct_table.call_date_time, "10 minutes", "5 minutes"),
        psf.col("original_crime_type_name"),
        psf.col("disposition")
    ) \
        .count()

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    query = agg_df.writeStream.format("console").outputMode("complete").queryName("aggregate").start()


    # TODO attach a ProgressReporter
    query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)
    #radio_code_df.printSchema();

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    radio_code_df.printSchema();
    agg_df.printSchema();

    # TODO join on disposition column
    #join_query = agg_df.writeStream \
    #        .format("console") \
    #        .start()
    join_query = agg_df \
        .join(radio_code_df, "disposition") \
        .writeStream \
        .format("console") \
        .outputMode("complete")\
        .queryName("join") \
        .start()



    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port",3000) \
        .config("spark.sql.shuffle.partitions", "30") \
        .config("spark.default.parallelism", "6") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
