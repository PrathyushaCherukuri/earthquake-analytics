import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

HISTORY_PATH = "s3://prathyusha-project/curated/earthquakes_history/"
LATEST_PATH = "s3://prathyusha-project/curated/earthquakes_latest/"

df = spark.read.parquet(HISTORY_PATH)

w = Window.partitionBy("quake_id").orderBy(col("updated_ms").desc())

latest = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)

(latest.write
    .mode("overwrite")
    .parquet(LATEST_PATH)
)

job.commit()
