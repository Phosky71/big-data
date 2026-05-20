"""
Transformación Capa Bronze a Silver.

Lee datos raw desde HDFS Bronze, aplica validaciones de calidad con Spark,
tipa columnas, elimina duplicados y escribe únicamente registros válidos
en MinIO (capa Silver) en formato Parquet particionado.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour
)

BRONZE_PATH = "hdfs://namenode:9000/datalake/bronze/iot/sensor_aula_01/year=2026/month=04/day=27/sensor_aula_01_raw.jsonl"
SILVER_PATH = "s3a://datalake/silver/iot/sensor_aula_01/"


def main():
    print("🚀 Iniciando transformación de Bronze a Silver...")

    # Configurar SparkSession con acceso a HDFS y MinIO (S3A)
    spark = (
        SparkSession.builder
        .appName("IoT_Bronze_to_Silver")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    print(f"📂 Leyendo datos raw desde Bronze: {BRONZE_PATH}")
    df_raw = spark.read.json(BRONZE_PATH)

    # Tipado de columnas
    df_typed = (
        df_raw
        .withColumn("timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("temperature_d", col("temperature").cast("double"))
        .withColumn("humidity_d", col("humidity").cast("double"))
        .withColumn("co2_i", col("co2").cast("integer"))
        .withColumn("battery_i", col("battery").cast("integer"))
    )

    # Filtrado de calidad: aplicar mismas reglas que validate_raw.py
    df_valid = (
        df_typed
        .filter(col("event_id").isNotNull())
        .filter(col("device_id").isNotNull())
        .filter(col("timestamp_ts").isNotNull())
        .filter(col("temperature_d").isNotNull())
        .filter(col("humidity_d").isNotNull())
        .filter(col("co2_i").isNotNull())
        .filter(col("battery_i").isNotNull())
        .filter((col("temperature_d") >= -20) & (col("temperature_d") <= 80))
        .filter((col("humidity_d") >= 0) & (col("humidity_d") <= 100))
        .filter(col("co2_i") > 0)
        .filter((col("battery_i") >= 0) & (col("battery_i") <= 100))
        .dropDuplicates(["event_id"])
    )

    # Renombrar y añadir columnas de particionado
    df_silver = (
        df_valid
        .select(
            col("event_id"),
            col("device_id"),
            col("timestamp_ts").alias("timestamp"),
            col("temperature_d").alias("temperature"),
            col("humidity_d").alias("humidity"),
            col("co2_i").alias("co2"),
            col("battery_i").alias("battery"),
            col("status")
        )
        .withColumn("year", year(col("timestamp")))
        .withColumn("month", month(col("timestamp")))
        .withColumn("day", dayofmonth(col("timestamp")))
        .withColumn("hour", hour(col("timestamp")))
    )

    print(f"📊 Registros válidos después de limpieza: {df_silver.count()}")
    print(f"💾 Escribiendo capa Silver en MinIO: {SILVER_PATH}")

    (
        df_silver.write
        .mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(SILVER_PATH)
    )

    print("✅ Transformación a Silver completada con éxito.")
    spark.stop()


if __name__ == "__main__":
    main()