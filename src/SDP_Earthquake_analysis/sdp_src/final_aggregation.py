from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import *


@dp.table(
    name = 'lakehouse.`04_gold`.sdp_geospatial_hotspot'
    , comment="gold table form sdp"
)
def sdp_geospatial_hotspot():
    source_df = spark.read.table('lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream')

    event_density = (
     source_df
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
    name = 'lakehouse.`04_gold`.sdp_seisemic_activity'
    , comment="gold table form sdp on seisemic_activity"
)
def sdp_seisemic_activity():
    source_df = spark.read.table('lakehouse.`03_silver`.earthquake_data_final_with_cdc_stream')

    seismic_activity = (
        source_df
        .withColumn("activity_houe", F.date_trunc("hour", F.col("time")))
        .groupBy("activity_houe","net")
        .agg(
            F.count("hash_id").alias("total_event"),
            F.avg("mag").alias("avg_mag"),
            F.max("mag").alias("max_mag")
        )
        .withColumn('hash_id', F.sha2(F.concat_ws('_', F.col('activity_houe'), F.col('net')), 256).substr(0,15))
    )
    
    return seismic_activity