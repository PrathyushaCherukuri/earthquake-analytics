import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from pyspark.sql.functions import (
    col,
    from_unixtime,
    input_file_name,
    regexp_extract
)

# --------------------
# Glue boilerplate
# --------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_PATH = "s3://prathyusha-project/raw/earthquakes/"
CURATED_PATH = "s3://prathyusha-project/curated/earthquakes_history/"

# --------------------
# 1) Read raw JSON
# --------------------
df = spark.read.json(RAW_PATH)

# --------------------
# 2) Extract dt/hour from S3 path
# --------------------
df = df.withColumn("_file", input_file_name())
df = df.withColumn("dt", regexp_extract(col("_file"), r"dt=([0-9\\-]+)", 1))
df = df.withColumn("hour", regexp_extract(col("_file"), r"hour=([0-9]+)", 1))
df = df.filter((col("dt") != "") & (col("hour") != ""))

# --------------------
# 3) Flatten schema
# --------------------
flat = df.select(
    col("source"),
    col("ingested_at_epoch_ms").cast("long"),

    col("feature.id").alias("quake_id"),

    col("feature.properties.updated").cast("long").alias("updated_ms"),
    col("feature.properties.time").cast("long").alias("event_time_ms"),

    col("feature.properties.mag").cast("double").alias("mag"),
    col("feature.properties.place").cast("string").alias("place"),
    col("feature.properties.title").cast("string").alias("title"),
    col("feature.properties.url").cast("string").alias("event_url"),

    col("feature.geometry.coordinates")[0].cast("double").alias("lon"),
    col("feature.geometry.coordinates")[1].cast("double").alias("lat"),
    col("feature.geometry.coordinates")[2].cast("double").alias("depth_km"),

    col("dt"),
    col("hour")
)

# --------------------
# 4) Clean
# --------------------
flat = flat.filter(col("quake_id").isNotNull())
flat = flat.fillna({"mag": 0.0})

# --------------------
# 5) Add readable timestamps
# --------------------
final_df = (
    flat
    .withColumn("event_time", from_unixtime(col("event_time_ms") / 1000))
    .withColumn("updated_time", from_unixtime(col("updated_ms") / 1000))
)

# --------------------
# 6) Write HISTORY (append only)
# --------------------
(
    final_df.write
    .mode("append")
    .partitionBy("dt", "hour")
    .parquet(CURATED_PATH)
)

job.commit()
