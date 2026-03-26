import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

# Environment setup
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

schema = StructType([
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("title", StringType(), True),
    StructField("wiki", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("domain", StringType(), True),
])

# Use the project directory
BASE_DIR = os.path.abspath("data")
LAKE_PATH = os.path.join(BASE_DIR, "lake/wiki_edits")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoints/wiki_edits")

def create_spark_session():
    return SparkSession.builder \
        .appName("WikiStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    
    os.makedirs(LAKE_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wiki_edits") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("id").isNotNull())

    # Pure local write — no Kestra calls
    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", LAKE_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="30 seconds") \
        .start()

    print("🚀 Spark is writing to the local Data Lake...")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()