from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Ingest").getOrCreate()
df = spark.read.option("header", True).csv("dbfs:/mnt/raw/sample_data.csv")
df.write.format("delta").mode("overwrite").save("dbfs:/mnt/bronze/sample_data")
print("Ingestion completed.")