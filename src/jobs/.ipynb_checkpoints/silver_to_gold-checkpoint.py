"""
Transformación Capa Plata a Oro.
Lee los datos limpios y tipados de MinIO, realiza agregaciones temporales (por hora)
y genera un dataset orientado a la visualización y análisis de incidencias.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, count, sum, when, round

# Rutas de origen y destino en MinIO
SILVER_PATH = "s3a://datalake/silver/iot/sensor_aula_01/"
GOLD_PATH = "s3a://datalake/gold/iot/sensor_aula_01/hourly_metrics/"

def main():
    print("🚀 Iniciando construcción de la capa Oro...")

    # 1. Crear sesión de Spark (con conexión a MinIO)
    spark = SparkSession.builder \
        .appName("IoT_Silver_to_Gold") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # 2. Leer los datos desde la capa Plata
    print(f"Leyendo capa Plata desde: {SILVER_PATH}")
    df_silver = spark.read.parquet(SILVER_PATH)

    # 3. Agregaciones orientadas al negocio (agrupado por año, mes, día y hora)
    # Definimos umbrales para contar incidencias: Temperatura > 26°C o CO2 > 1000 ppm o status = WARNING
    df_gold = df_silver.groupBy("year", "month", "day", "hour").agg(
        # Métricas de Temperatura
        round(avg("temperature"), 2).alias("avg_temperature"),
        max("temperature").alias("max_temperature"),
        min("temperature").alias("min_temperature"),
        
        # Métricas de Humedad y CO2
        round(avg("humidity"), 2).alias("avg_humidity"),
        round(avg("co2"), 0).alias("avg_co2"),
        
        # Métrica de Batería (nos interesa el valor mínimo de la hora)
        min("battery").alias("min_battery"),
        
        # Conteo de eventos (Volumen)
        count("event_id").alias("total_events"),
        
        # Detección de incidencias/anomalías controladas
        sum(when(col("temperature") > 26.0, 1).otherwise(0)).alias("alerts_high_temp"),
        sum(when(col("co2") > 1000, 1).otherwise(0)).alias("alerts_high_co2"),
        sum(when(col("status") == "WARNING", 1).otherwise(0)).alias("alerts_warning_status")
    )

    # 4. Escribir dataset final en MinIO en formato Parquet
    print(f"Escribiendo capa Oro en MinIO: {GOLD_PATH}")
    df_gold.write \
        .mode("overwrite") \
        .parquet(GOLD_PATH)

    print("✅ Construcción de capa Oro completada con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()