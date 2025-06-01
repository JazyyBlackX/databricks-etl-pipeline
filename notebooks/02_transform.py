from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("Transform").getOrCreate()
df = spark.read.format("delta").load("dbfs:/mnt/bronze/sample_data")
df_clean = df.dropna().withColumn("date", to_date(col("timestamp")))
df_clean.write.format("delta").mode("overwrite").save("dbfs:/mnt/silver/sample_data")
print("Transformation completed.")