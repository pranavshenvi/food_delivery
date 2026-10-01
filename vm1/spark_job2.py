"""VM1 -- Spark Job 2 (operations analytics): order_metrics_landed.jsonl -> restaurant_summary.csv + city_summary.csv.
Usage: python spark_job2.py [--input order_metrics_landed.jsonl]"""
import argparse
import glob
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

ap = argparse.ArgumentParser()
ap.add_argument("--input", default="order_metrics_landed.jsonl")
a = ap.parse_args()

spark = SparkSession.builder.master("local[*]").config("spark.hadoop.fs.defaultFS", "file:///").appName("job2-ops-analytics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([StructField(c, StringType()) for c in
                     ["order_id", "customer_id", "restaurant_id", "delivery_partner_id", "city", "delivery_status", "delay_category"]]
                    + [StructField(c, DoubleType()) for c in
                       ["order_amount", "delivery_fee", "distance_km", "preparation_time_min", "delivery_time_min",
                        "fulfillment_time_min", "delivery_speed_kmph", "delay_min"]])

raw = spark.read.text("file://" + os.path.abspath(a.input)).where(F.trim("value") != "")
df = (raw.select(F.from_json("value", schema).alias("r")).select("r.*")
      .where(F.col("order_id").isNotNull())
      .dropDuplicates())  # exact duplicates counted once

delayed = F.sum(F.when(F.col("delivery_status") == "DELAYED", 1).otherwise(0))


def write_csv(frame, path):
    tmp = path + ".tmp_dir"
    shutil.rmtree(tmp, ignore_errors=True)
    frame.coalesce(1).write.option("header", True).csv(tmp)
    shutil.move(glob.glob(os.path.join(tmp, "part-*.csv"))[0], path)
    shutil.rmtree(tmp)
    print(f"wrote {frame.count()} rows to {path}")


restaurant = (df.groupBy("restaurant_id").agg(
    F.count("*").alias("order_count"),
    F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
    F.round(F.avg("preparation_time_min"), 2).alias("avg_preparation_time_min"),
    F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
    delayed.alias("delayed_order_count"),
    F.round(100.0 * delayed / F.count("*"), 2).alias("delay_rate_pct")).orderBy("restaurant_id"))

city = (df.groupBy("city").agg(
    F.count("*").alias("order_count"),
    F.round(F.avg("order_amount"), 2).alias("avg_order_amount"),
    F.round(F.avg("delivery_time_min"), 2).alias("avg_delivery_time_min"),
    delayed.alias("delayed_order_count"),
    F.round(100.0 * delayed / F.count("*"), 2).alias("delay_rate_pct")).orderBy("city"))

write_csv(restaurant, "restaurant_summary.csv")
write_csv(city, "city_summary.csv")
spark.stop()
