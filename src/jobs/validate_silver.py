"""
Validación básica de la capa Silver.

Comprueba que los datos en Silver cumplen con las reglas de calidad
y genera un informe de validación.
"""

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

SILVER_PATH = "s3a://datalake/silver/iot/sensor_aula_01/"
OUTPUT_DIR = "/home/jovyan/work/src/data/reports"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "silver_validation_report.json")


def main():
    print("🚀 Iniciando validación de la capa Silver...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("IoT_Validate_Silver")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    print(f"📂 Leyendo capa Silver: {SILVER_PATH}")
    df = spark.read.parquet(SILVER_PATH)

    # Validaciones de calidad sobre Silver
    total_records = df.count()
    null_event_id = df.filter(col("event_id").isNull()).count()
    null_device_id = df.filter(col("device_id").isNull()).count()
    null_timestamp = df.filter(col("timestamp").isNull()).count()
    bad_temperature = df.filter((col("temperature") < -20) | (col("temperature") > 80)).count()
    bad_humidity = df.filter((col("humidity") < 0) | (col("humidity") > 100)).count()
    bad_co2 = df.filter(col("co2") <= 0).count()
    bad_battery = df.filter((col("battery") < 0) | (col("battery") > 100)).count()
    duplicate_event_ids = total_records - df.select("event_id").distinct().count()

    # Determinar si Silver pasó validación
    is_valid = all([
        null_event_id == 0,
        null_device_id == 0,
        null_timestamp == 0,
        bad_temperature == 0,
        bad_humidity == 0,
        bad_co2 == 0,
        bad_battery == 0,
        duplicate_event_ids == 0
    ])

    report = {
        "silver_path": SILVER_PATH,
        "total_records": total_records,
        "null_event_id": null_event_id,
        "null_device_id": null_device_id,
        "null_timestamp": null_timestamp,
        "bad_temperature": bad_temperature,
        "bad_humidity": bad_humidity,
        "bad_co2": bad_co2,
        "bad_battery": bad_battery,
        "duplicate_event_ids": duplicate_event_ids,
        "silver_validation_passed": is_valid
    }

    # Guardar informe
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4, ensure_ascii=False)

    # Resumen por consola
    print("✅ Validación Silver completada.")
    print(f"   Total registros: {total_records}")
    print(f"   Validación pasada: {'SÍ' if is_valid else 'NO'}")
    print(f"\n📄 Informe guardado en: {OUTPUT_FILE}")
    print(json.dumps(report, indent=2, ensure_ascii=False))

    spark.stop()


if __name__ == "__main__":
    main()