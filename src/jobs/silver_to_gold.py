"""
Transformación Capa Silver a Gold.

Genera agregaciones por hora útiles para análisis temporal del dispositivo:
- Métricas promedio, máxima y mínima por hora
- Conteo de eventos
- Detección de anomalías (batería baja, CO2 alto, temperaturas extremas)
- Evolución temporal del dispositivo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    when
)

SILVER_PATH = "s3a://datalake/silver/iot/sensor_aula_01/"
GOLD_PATH = "s3a://datalake/gold/iot/sensor_aula_01/hourly_metrics/"


def main():
    print("🚀 Iniciando transformación Silver a Gold...")

    spark = (
        SparkSession.builder
        .appName("IoT_Silver_to_Gold")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    print(f"📂 Leyendo capa Silver: {SILVER_PATH}")
    df_silver = spark.read.parquet(SILVER_PATH)

    # Crear columna date_hour para agregación temporal
    df_with_hour = df_silver.withColumn(
        "date_hour",
        date_format(col("timestamp"), "yyyy-MM-dd HH:00:00")
    )

    # Definir anomalías
    df_with_anomalies = df_with_hour.withColumn(
        "anomaly_low_battery",
        when(col("battery") < 20, 1).otherwise(0)
    ).withColumn(
        "anomaly_high_co2",
        when(col("co2") > 1000, 1).otherwise(0)
    ).withColumn(
        "anomaly_high_temp",
        when(col("temperature") > 26, 1).otherwise(0)
    ).withColumn(
        "anomaly_low_temp",
        when(col("temperature") < 20, 1).otherwise(0)
    ).withColumn(
        "anomaly_high_humidity",
        when(col("humidity") > 70, 1).otherwise(0)
    ).withColumn(
        "anomaly_low_humidity",
        when(col("humidity") < 40, 1).otherwise(0)
    )

    # Agregaciones por hora
    df_gold = df_with_anomalies.groupBy("date_hour").agg(
        count("event_id").alias("num_events"),
        
        # Temperatura
        avg("temperature").alias("avg_temperature"),
        spark_max("temperature").alias("max_temperature"),
        spark_min("temperature").alias("min_temperature"),
        
        # Humedad
        avg("humidity").alias("avg_humidity"),
        spark_max("humidity").alias("max_humidity"),
        spark_min("humidity").alias("min_humidity"),
        
        # CO2
        avg("co2").alias("avg_co2"),
        spark_max("co2").alias("max_co2"),
        spark_min("co2").alias("min_co2"),
        
        # Batería
        avg("battery").alias("avg_battery"),
        spark_max("battery").alias("max_battery"),
        spark_min("battery").alias("min_battery"),
        
        # Anomalías
        spark_sum("anomaly_low_battery").alias("num_low_battery"),
        spark_sum("anomaly_high_co2").alias("num_high_co2"),
        spark_sum("anomaly_high_temp").alias("num_high_temp"),
        spark_sum("anomaly_low_temp").alias("num_low_temp"),
        spark_sum("anomaly_high_humidity").alias("num_high_humidity"),
        spark_sum("anomaly_low_humidity").alias("num_low_humidity")
    ).orderBy("date_hour")

    # Calcular total de anomalías por hora
    df_gold = df_gold.withColumn(
        "total_anomalies",
        col("num_low_battery") + 
        col("num_high_co2") + 
        col("num_high_temp") + 
        col("num_low_temp") +
        col("num_high_humidity") +
        col("num_low_humidity")
    )

    print(f"📊 Registros agregados en Gold: {df_gold.count()}")
    print(f"💾 Escribiendo capa Gold en MinIO: {GOLD_PATH}")

    df_gold.write.mode("overwrite").parquet(GOLD_PATH)

    # Mostrar muestra de resultados
    print("\n📋 Muestra de agregaciones por hora:")
    df_gold.select(
        "date_hour",
        "num_events",
        "avg_temperature",
        "avg_humidity",
        "avg_co2",
        "avg_battery",
        "total_anomalies"
    ).show(10, truncate=False)

    print("✅ Transformación a Gold completada con éxito.")
    spark.stop()


if __name__ == "__main__":
    main()