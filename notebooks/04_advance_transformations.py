from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, sum as _sum

spark = SparkSession.builder.appName("AdvancedTransformations").getOrCreate()

# Load data
df = spark.read.format("delta").load("dbfs:/mnt/silver/sample_data")

# Window function: Deduplicate based on latest timestamp per user
window_spec = Window.partitionBy("user_id").orderBy(col("timestamp").desc())
df_dedup = df.withColumn("row_num", row_number().over(window_spec)).filter("row_num = 1")

# Aggregation: Total value per category
df_agg = df.groupBy("category").agg(_sum("amount").alias("total_amount"))

# Join: Combine deduplicated with aggregates
df_final = df_dedup.join(df_agg, on="category", how="left")

# Save final dataset
df_final.write.format("delta").mode("overwrite").save("dbfs:/mnt/gold/final_data")

print("Advanced transformation and join completed.")
