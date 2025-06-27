import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, sqrt, pow, row_number, avg, window
from pyspark.sql.types import *
from pyspark.sql.window import Window

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

# Load region lookup (bundesland, lat, lon)
region_lookup = spark.read.csv("region_lookup.csv", header=True, inferSchema=True)

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

# -- Region enrichment: join on nearest centroid (demo) --
joined = exploded.crossJoin(region_lookup)
joined = joined.withColumn("distance",
    sqrt(
        pow(col("latitude") - col("lat"), 2) +
        pow(col("longitude") - col("lon"), 2)
    )
)

w = Window.partitionBy("sensorsId").orderBy(col("distance"))
nearest = joined.withColumn("rn", row_number().over(w)).filter(col("rn") == 1)

# -- Windowed aggregation by Bundesland, pollutant, and time window --
agg = nearest.groupBy(
    window(col("timestamp").cast("timestamp"), "5 minutes"),
    col("bundesland"),
    col("parameter")
).agg(
    avg("value").alias("avg_value")
)

# -- Output to console --
query = agg.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .start()

query.awaitTermination()

