"""L1: python batch_job.py   -- run once, after the producer has finished and L4's stream has caught up.
Batch Spark (not streaming): reads a fixed snapshot of `flagged_transactions` -- whatever's in the
topic right now, offsets earliest..latest -- and summarizes it. Contrast with fraud_job.py on L4,
which runs continuously with a trigger and a checkpoint; this runs once and exits."""
import pyspark
from pyspark.sql import SparkSession, functions as F

BOOTSTRAP = "172.22.134.139:9092,172.22.114.189:9092"  # <-- the two Kafka brokers

ALERT_SCHEMA = "alert_id STRING, rule STRING, card_id INT, txn_id STRING, event_time STRING, details STRING"


def main():
    scala = "2.13" if pyspark.__version__ >= "4" else "2.12"
    spark = (SparkSession.builder.appName("fraud-batch-report")
             .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", "flagged_transactions")
           .option("startingOffsets", "earliest")
           .option("endingOffsets", "latest")
           .load())
    alerts = raw.select(F.from_json(F.col("value").cast("string"), ALERT_SCHEMA).alias("a")).select("a.*")
    alerts.cache()

    total = alerts.count()
    print(f"=== batch report: {total} flagged transactions collected ===")

    print("\n--- by rule ---")
    alerts.groupBy("rule").count().orderBy("rule").show(truncate=False)

    print("\n--- top 10 most-flagged cards ---")
    (alerts.groupBy("card_id").count()
     .orderBy(F.desc("count"))
     .limit(10)
     .show(truncate=False))

    out_path = "flagged_summary"
    (alerts.orderBy("event_time")
     .coalesce(1)
     .write.mode("overwrite").option("header", True).csv(out_path))
    print(f"\nwrote per-alert detail to ./{out_path}/")

    spark.stop()


if __name__ == "__main__":
    main()
