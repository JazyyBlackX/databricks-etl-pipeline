from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

df = spark.read.format("delta").load("dbfs:/mnt/gold/final_data")

# Check for nulls
null_counts = df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).agg(*[sum(col(c)).alias(c) for c in df.columns])
null_counts.show()

# Check for duplicate rows
duplicate_count = df.groupBy(df.columns).count().filter("count > 1").count()
print(f"Duplicate row count: {duplicate_count}")

# Validate value ranges
df.filter("amount < 0 OR amount > 100000").show()
