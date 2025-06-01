from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Load").getOrCreate()
df = spark.read.format("delta").load("dbfs:/mnt/silver/sample_data")
df.groupBy("category").count().write.format("delta").mode("overwrite").save("dbfs:/mnt/gold/sample_agg")
print("Loading completed.")