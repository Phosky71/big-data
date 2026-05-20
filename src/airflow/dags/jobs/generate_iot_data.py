"""
Generador de datos IoT simulados para un único dispositivo.

Genera registros JSONL con errores controlados para probar reglas de calidad:
- valores fuera de rango
- timestamps nulos o mal formados
- device_id ausente
- temperatura nula
- duplicados de event_id
"""

import json
import os
import random
from datetime import datetime, timedelta

DEVICE_ID = "sensor_aula_01"
NUM_RECORDS = 500
ERROR_RATE = 0.10
START_TIME = datetime(2026, 4, 27, 8, 0, 0)
INTERVAL_SEC = 60

# Ruta accesible desde Airflow y compartida en el proyecto
OUTPUT_DIR = "/opt/airflow/dags/data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "sensor_aula_01_raw.jsonl")

ERROR_TYPES = [
    "temp_out_of_range",
    "humidity_negative",
    "battery_invalid",
    "null_temperature",
    "null_timestamp",
    "bad_timestamp",
    "co2_negative",
    "missing_device_id",
]


def gen_normal_record(event_num, ts):
    """Genera un registro IoT válido."""
    return {
        "event_id": f"evt_{event_num:06d}",
        "device_id": DEVICE_ID,
        "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S"),
        "temperature": round(random.uniform(19.0, 28.0), 2),
        "humidity": round(random.uniform(35.0, 75.0), 2),
        "co2": random.randint(400, 1200),
        "battery": max(0, 100 - int(event_num * 0.12)),
        "status": random.choice(["OK", "OK", "OK", "WARNING"]),
    }


def inject_error(record, error_type):
    """Inyecta un error controlado en un registro."""
    r = dict(record)

    if error_type == "temp_out_of_range":
        r["temperature"] = random.choice([150.0, -50.0, 999.9])
        r["status"] = "ERROR"

    elif error_type == "humidity_negative":
        r["humidity"] = round(random.uniform(-20.0, -1.0), 2)

    elif error_type == "battery_invalid":
        r["battery"] = random.choice([-5, 130, 999])

    elif error_type == "null_temperature":
        r["temperature"] = None

    elif error_type == "null_timestamp":
        r["timestamp"] = ""

    elif error_type == "bad_timestamp":
        r["timestamp"] = "not-a-date"

    elif error_type == "co2_negative":
        r["co2"] = random.choice([-100, 0, -1])

    elif error_type == "missing_device_id":
        r["device_id"] = None

    return r


def main():
    print("🚀 Iniciando generación de datos IoT...")
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    random.seed(42)

    records = []
    error_event_ids = set()
    current_ts = START_TIME

    # Generar registros base
    for i in range(1, NUM_RECORDS + 1):
        record = gen_normal_record(i, current_ts)

        # Inyectar errores controlados
        if random.random() < ERROR_RATE:
            error_type = random.choice(ERROR_TYPES)
            record = inject_error(record, error_type)
            error_event_ids.add(record["event_id"])

        records.append(record)
        current_ts += timedelta(seconds=INTERVAL_SEC)

    # Inyectar duplicados
    duplicate_sources = random.sample(records[:100], 5)
    duplicates = [dict(r) for r in duplicate_sources]
    records.extend(duplicates)
    
    # Mezclar para simular ingesta desordenada
    random.shuffle(records)

    # Escribir JSONL
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Resumen
    total = len(records)
    errors = sum(1 for r in records if r.get("event_id") in error_event_ids)
    dups = len(duplicates)
    estimated_valid = total - errors - dups

    print(f"✅ Generados {total} registros en {OUTPUT_FILE}")
    print(f"   Registros con error inyectado : {errors}")
    print(f"   Duplicados inyectados         : {dups}")
    print(f"   Registros válidos estimados   : {estimated_valid}")


if __name__ == "__main__":
    main()