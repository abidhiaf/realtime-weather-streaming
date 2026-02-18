from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when
from pyspark.sql.types import StructType, StringType, DoubleType

# 1️⃣ Créer la session Spark
spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .getOrCreate()

# 2️⃣ Lire le flux depuis Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_api_events") \
    .option("startingOffsets", "earliest") \
    .load()

# 3️⃣ Convertir la valeur en string
df_values = df_kafka.selectExpr("CAST(value AS STRING) as json_str")

# 4️⃣ Schéma correspondant au producer
schema = StructType() \
    .add("city", StringType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("pressure", DoubleType()) \
    .add("event_time", StringType())

df_parsed = df_values.withColumn(
    "data",
    from_json(col("json_str"), schema)
).select("data.*")

# ⚡ Ajouter une colonne calculée (temp_diff = 0 pour l'instant)
df_transformed = df_parsed.withColumn(
    "temp_diff", col("temperature") - col("temperature")
)

# ⚡ Ajouter un indicateur d'alerte si temperature > 30°C
df_transformed = df_transformed.withColumn(
    "alert_temp",
    when(col("temperature") > 30, "High temp").otherwise("Normal")
)

# ✅ Écrire le flux transformé dans Parquet
query = df_transformed.writeStream \
    .format("parquet") \
    .option("path", "data/weather_lake") \
    .option("checkpointLocation", "data/checkpoints") \
    .outputMode("append") \
    .start()

query.awaitTermination()
