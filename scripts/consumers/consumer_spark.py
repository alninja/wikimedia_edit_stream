import os
import requests
import shutil
import time

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

schema = StructType([
    StructField("id",        LongType(),    True),
    StructField("type",      StringType(),  True),
    StructField("user",      StringType(),  True),
    StructField("bot",       BooleanType(), True),
    StructField("title",     StringType(),  True),
    StructField("wiki",      StringType(),  True),
    StructField("timestamp", LongType(),    True),
    StructField("domain",    StringType(),  True),
])

BASE_DIR = os.path.abspath("data")
LAKE_PATH = os.path.join(BASE_DIR, "lake/wiki_edits")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoints/wiki_edits")

KESTRA_WEBHOOK_URL = (
    "http://localhost:8099/api/v1/executions/"
    "zoomcamp.wikimedia/wiki_batch_to_gcs_bq"  # executions endpoint, not webhook
)

KESTRA_BASE_URL = "http://localhost:8099"
KESTRA_NAMESPACE = "zoomcamp.wikimedia"
KESTRA_FLOW_ID = "wiki_batch_to_gcs_bq"

def push_to_kestra(file_path, batch_id, retries=3, delay=5):
    storage_path = f"/wiki_edits/batch_{batch_id}.parquet"
    internal_uri = f"kestra:///{KESTRA_NAMESPACE}{storage_path}"
    upload_url   = f"{KESTRA_BASE_URL}/api/v1/namespaces/{KESTRA_NAMESPACE}/files?path={storage_path}"
    exec_url     = f"{KESTRA_BASE_URL}/api/v1/executions/{KESTRA_NAMESPACE}/{KESTRA_FLOW_ID}"

    for attempt in range(1, retries + 1):
        try:
            # Step 1 — upload file to Kestra namespace storage
            with open(file_path, 'rb') as f:
                upload_response = requests.post(
                    upload_url,
                    files={'fileContent': (os.path.basename(file_path), f, 'application/octet-stream')},
                    timeout=30
                )

            if upload_response.status_code != 200:
                print(f"⚠️ Upload failed (attempt {attempt}/{retries}): {upload_response.status_code} - {upload_response.text}")
                time.sleep(delay)
                continue

            print(f"📁 Uploaded to Kestra storage: {internal_uri}")

            # Step 2 — trigger execution with internal URI as form field
            exec_response = requests.post(
                exec_url,
                files={
                "wiki_file": (None, internal_uri),
            "batch_id": (None, str(batch_id))},  # multipart with string value
                timeout=30
            )

            if exec_response.status_code == 200:
                exec_id = exec_response.json().get('id', 'unknown')
                print(f"✅ Execution triggered: {exec_id}")
                return True
            else:
                print(f"⚠️ Execution failed (attempt {attempt}/{retries}): {exec_response.status_code} - {exec_response.text}")

        except Exception as e:
            print(f"⚠️ Error (attempt {attempt}/{retries}): {e}")

        time.sleep(delay)    

    print(f"❌ Failed batch {batch_id} after {retries} attempts")
    return False

def process_batch(df, batch_id):
    print(f"Batch {batch_id}: processing...")
    batch_export_path = os.path.join(LAKE_PATH, f"batch_{batch_id}")

    try:
        df.coalesce(1).write.mode("overwrite").parquet(batch_export_path)

        parquet_file = next(
            (os.path.join(batch_export_path, f)
             for f in os.listdir(batch_export_path)
             if f.endswith(".parquet") and not f.startswith("_")),
            None
        )

        if parquet_file:
            print(f"Batch {batch_id}: pushing {os.path.basename(parquet_file)} to Kestra...")
            push_to_kestra(parquet_file, batch_id)
        else:
            print(f"Batch {batch_id}: no parquet file found.")

    finally:
        if os.path.exists(batch_export_path):
            shutil.rmtree(batch_export_path)
            print(f"Batch {batch_id}: cleaned up.")

def create_spark_session():
    from pyspark import SparkContext
    # Stop any existing context before creating a new one
    existing = SparkContext._active_spark_context
    if existing:
        existing.stop()

    return SparkSession.builder \
        .appName("WikiStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(LAKE_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wiki_edits") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("kafka.request.timeout.ms", "120000") \
        .option("kafka.session.timeout.ms", "120000") \
        .option("kafka.max.poll.interval.ms", "120000") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("id").isNotNull())

    query = parsed_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("🚀 Stream processing and Kestra push started...")
    query.awaitTermination()

if __name__ == "__main__":
    process_stream()