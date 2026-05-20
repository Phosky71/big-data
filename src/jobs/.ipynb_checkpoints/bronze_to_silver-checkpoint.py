"""
Transformación Capa Bronze a Plata.
Lee los datos validados desde HDFS, tipa las columnas, genera particiones
y guarda el resultado en MinIO en formato Parquet.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour

# Rutas de origen y destino
# Nota: Ajusta la fecha si lo haces dinámico, aquí usamos la de tu generador
BRONZE_PATH = "hdfs:///datalake/bronze/iot/sensor_aula_01/year=2026/month=04/day=27/sensor_aula_01_valid.jsonl"
SILVER_PATH = "s3a://datalake/silver/iot/sensor_aula_01/"

def main():
    print("🚀 Iniciando transformación de Bronze a Plata...")

    # 1. Crear sesión de Spark
    # Las configuraciones de S3a suelen venir del entorno (spark-defaults.conf), 
    # pero las incluimos explícitamente para asegurar la conexión a MinIO.
    spark = SparkSession.builder \
        .appName("IoT_Bronze_to_Silver") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # 2. Leer los datos desde HDFS
    print(f"Leyendo datos válidos desde: {BRONZE_PATH}")
    df_raw = spark.read.json(BRONZE_PATH)

    # 3. Tipado y Normalización
    # Convertimos strings a tipos nativos para optimizar el Parquet
    df_silver = df_raw.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")) \
                      .withColumn("temperature", col("temperature").cast("double")) \
                      .withColumn("humidity", col("humidity").cast("double")) \
                      .withColumn("co2", col("co2").cast("integer")) \
                      .withColumn("battery", col("battery").cast("integer"))

    # 4. Enriquecimiento para particionado
    # Extraemos año, mes, día y hora para facilitar consultas analíticas posteriores
    df_silver = df_silver.withColumn("year", year(col("timestamp"))) \
                         .withColumn("month", month(col("timestamp"))) \
                         .withColumn("day", dayofmonth(col("timestamp"))) \
                         .withColumn("hour", hour(col("timestamp")))

    # (Opcional pero recomendado): Eliminar posibles duplicados exactos 
    # aunque ya los limpiamos en Python, esto asegura idempotencia en Spark
    df_silver = df_silver.dropDuplicates(["event_id"])

    # 5. Escribir en MinIO en formato Parquet
    print(f"Escribiendo capa Plata en MinIO: {SILVER_PATH}")
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .parquet(SILVER_PATH)

    print("✅ Transformación a Plata completada con éxito.")
    spark.stop()

if __name__ == "__main__":
    main()