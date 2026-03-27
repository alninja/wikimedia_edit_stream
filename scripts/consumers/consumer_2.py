import os
import time
import threading
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, LongType

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

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

BASE_DIR       = os.path.abspath("data")
LAKE_PATH      = os.path.join(BASE_DIR, "lake/wiki_edits")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "checkpoints/wiki_edits")

def count_parquet_files():
    """Count parquet files written to the lake."""
    if not os.path.exists(LAKE_PATH):
        return 0
    return len([f for f in os.listdir(LAKE_PATH) if f.endswith(".parquet")])

def get_lake_size_mb():
    """Get total size of the data lake in MB."""
    total = 0
    if os.path.exists(LAKE_PATH):
        for f in os.listdir(LAKE_PATH):
            fp = os.path.join(LAKE_PATH, f)
            if os.path.isfile(fp):
                total += os.path.getsize(fp)
    return total / (1024 * 1024)

def progress_monitor(query, stop_event):
    """Background thread that prints a live progress display."""
    total_rows = 0
    batch_count = 0
    spinner = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    spin_idx = 0

    print("\n" + "─" * 60)
    print("  🌊 Wiki Edits → Data Lake Pipeline")
    print("─" * 60)

    while not stop_event.is_set():
        progress = query.lastProgress

        if progress:
            rows_this_batch = progress.get("numInputRows", 0)
            total_rows += rows_this_batch
            batch_count += 1
            trigger_ms   = progress.get("triggerExecution", {}).get("totalMs", 0)
            rows_per_sec = progress.get("processedRowsPerSecond", 0.0)

            files    = count_parquet_files()
            size_mb  = get_lake_size_mb()
            now      = datetime.now().strftime("%H:%M:%S")
            spin     = spinner[spin_idx % len(spinner)]
            spin_idx += 1

            # Build progress bar based on rows (visual only)
            bar_fill  = min(int((total_rows % 1000) / 1000 * 20), 20)
            bar        = "█" * bar_fill + "░" * (20 - bar_fill)

            print(f"\r{spin} [{bar}] "
                  f"Batch #{batch_count:>4} | "
                  f"Rows: {total_rows:>7,} (+{rows_this_batch:>4}) | "
                  f"Speed: {rows_per_sec:>6.1f} r/s | "
                  f"Files: {files:>4} | "
                  f"Size: {size_mb:>6.2f} MB | "
                  f"{now}",
                  end="", flush=True)
        else:
            spin = spinner[spin_idx % len(spinner)]
            spin_idx += 1
            print(f"\r{spin} Waiting for first batch...", end="", flush=True)

        time.sleep(5)

    # Final summary on exit
    files   = count_parquet_files()
    size_mb = get_lake_size_mb()
    print(f"\n\n{'─' * 60}")
    print(f"  ✅ Pipeline stopped")
    print(f"  📦 Total rows processed : {total_rows:,}")
    print(f"  📁 Parquet files written : {files}")
    print(f"  💾 Data lake size        : {size_mb:.2f} MB")
    print(f"  📂 Location              : {LAKE_PATH}")
    print(f"{'─' * 60}\n")

def create_spark_session():
    return SparkSession.builder \
        .appName("WikiStreamConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.streaming.fileSource.cleaner.numThreads", "1") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .getOrCreate()
        

def process_stream():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")  # suppress WARN spam so progress shows cleanly

    os.makedirs(LAKE_PATH, exist_ok=True)
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wiki_edits") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("id").isNotNull())

    query = parsed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", LAKE_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="30 seconds") \
        .start()

    # Start progress monitor in background thread
    stop_event = threading.Event()
    monitor = threading.Thread(target=progress_monitor, args=(query, stop_event), daemon=True)
    monitor.start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\n⏹  Stopping pipeline...")
        query.stop()
    finally:
        stop_event.set()
        monitor.join(timeout=3)

if __name__ == "__main__":
    process_stream()
