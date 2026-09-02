from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window 

@dp.table(
    name = "lakehouse.`04_gold`.sdp_geospatial_hotspot",
    comment = "Gold table from sdp"
)

def sdp_geospatial_hotspot():
    source_dp = spark.read.table("lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream")

    event_density = (
        source_dp
        .groupBy(
            F.round(F.col('longitude'), 1).alias('avg_long'),
            F.round(F.col('latitude'), 1).alias('avg_lat'),
        )
        .agg(
            F.count("*").alias("event_density"),
            F.avg("mag").alias("avg_mag"),
            F.max("mag").alias("max_mag")
        )
        .withColumn('hash_id', F.sha2(F.concat_ws('_', F.col('avg_long'), F.col('avg_lat')), 256).substr(0,15))
    )

    return event_density

@dp.table(
    name = "lakehouse.`04_gold`.sdp_seismic_activity",
    comment = "Gold table from sdp on seismic activity"
)

def sdp_seismic_activity():
    source_dp = spark.read.table("lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream")

    seismic_activity = (
        source_dp
        .withColumn("activity_hour", F.date_trunc("hour", F.col("time")))
        .groupBy("activity_hour","net")
        .agg(
            F.count("hash_id").alias("total_event"),
            F.avg("mag").alias("avg_mag"),
            F.max("mag").alias("max_mag")
        )
        .withColumn('hash_id', F.sha2(F.concat_ws('_', F.col('activity_hour'), F.col('net')), 256).substr(0,15))
    )

    return seismic_activity





