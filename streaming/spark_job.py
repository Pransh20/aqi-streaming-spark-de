import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, avg, window, to_timestamp
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
TOPIC = "air_quality_de"

schema = StructType([
    StructField("parameter", StringType()),
    StructField("timestamp", LongType()),
    StructField("results", ArrayType(StructType([
        StructField("datetime", StructType([
            StructField("utc", StringType()),
            StructField("local", StringType())
        ])),
        StructField("value", DoubleType()),
        StructField("coordinates", StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType())
        ])),
        StructField("sensorsId", LongType()),
        StructField("locationsId", LongType())
    ]))),
])

spark = SparkSession.builder \
    .appName("AQI-Germany-Streaming") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Broadcast region lookup table
region_df = pd.read_csv("region_lookup.csv")
broadcast_region = spark.sparkContext.broadcast(region_df)

@pandas_udf(StringType())
def assign_bundesland(lat, lon):
    regions = broadcast_region.value
    assigned = []
    for la, lo in zip(lat, lon):
        dists = np.sqrt((regions["lat"] - la) ** 2 + (regions["lon"] - lo) ** 2)
        assigned.append(regions.loc[dists.idxmin(), "bundesland"])
    return pd.Series(assigned)

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.select(from_json(col("value").cast("string"), schema).alias("data"))
flat = json_df.select(
    col("data.parameter"),
    col("data.timestamp"),
    explode(col("data.results")).alias("result")
)

exploded = flat.select(
    col("parameter"),
    col("timestamp"),
    col("result.datetime.utc").alias("utc"),
    col("result.value").alias("value"),
    col("result.coordinates.latitude").alias("latitude"),
    col("result.coordinates.longitude").alias("longitude"),
    col("result.sensorsId").alias("sensorsId"),
    col("result.locationsId").alias("locationsId")
)

# Assign Bundesland using the pandas UDF
exploded = exploded.withColumn(
    "bundesland", assign_bundesland(col("latitude"), col("longitude"))
)

# Convert utc string to timestamp
exploded = exploded.withColumn("utc_ts", to_timestamp(col("utc")))

# Use watermark to enable append mode on timestamp column
agg = exploded.withWatermark("utc_ts", "10 minutes").groupBy(
    window(col("utc_ts"), "5 minutes"),
    col("bundesland"),
    col("parameter")
).agg(
    avg("value").alias("avg_value")
)

# Write to Parquet for visualization
query = agg.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "output/") \
    .option("checkpointLocation", "output/_checkpoint/") \
    .start()

query.awaitTermination()

