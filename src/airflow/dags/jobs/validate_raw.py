"""
Validación de calidad de datos IoT.

Lee el JSONL raw, aplica reglas de calidad y separa los datos en válidos e inválidos.
Genera un informe de calidad en JSON.
"""

import json
import os
from datetime import datetime

INPUT_FILE = "/opt/airflow/dags/data/raw/sensor_aula_01_raw.jsonl"
OUTPUT_DIR_VALID = "/opt/airflow/dags/data/valid"
OUTPUT_DIR_QUARANTINE = "/opt/airflow/dags/data/quarantine"

VALID_FILE = os.path.join(OUTPUT_DIR_VALID, "sensor_aula_01_valid.jsonl")
INVALID_FILE = os.path.join(OUTPUT_DIR_QUARANTINE, "sensor_aula_01_invalid.jsonl")
REPORT_FILE = os.path.join(OUTPUT_DIR_QUARANTINE, "quality_report.json")


def validate_date(date_text):
    """Valida que timestamp tenga formato correcto."""
    try:
        datetime.strptime(date_text, "%Y-%m-%dT%H:%M:%S")
        return True
    except (ValueError, TypeError):
        return False


def main():
    print("🚀 Iniciando validación de calidad de datos IoT...")
    
    os.makedirs(OUTPUT_DIR_VALID, exist_ok=True)
    os.makedirs(OUTPUT_DIR_QUARANTINE, exist_ok=True)

    stats = {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "duplicates": 0,
        "null_or_bad_timestamps": 0,
        "out_of_range_values": 0,
        "missing_mandatory": 0,
        "json_errors": 0
    }

    seen_events = set()
    valid_data = []
    invalid_data = []

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            stats["total_records"] += 1

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats["json_errors"] += 1
                stats["invalid_records"] += 1
                continue

            is_valid = True

            # Extraer campos
            event_id = record.get("event_id")
            device_id = record.get("device_id")
            timestamp = record.get("timestamp")
            temperature = record.get("temperature")
            humidity = record.get("humidity")
            co2 = record.get("co2")
            battery = record.get("battery")

            # Validación 1: Campos obligatorios
            if not event_id or not device_id:
                is_valid = False
                stats["missing_mandatory"] += 1

            # Validación 2: Duplicados
            elif event_id in seen_events:
                is_valid = False
                stats["duplicates"] += 1
            else:
                seen_events.add(event_id)

            # Validación 3: Timestamp correcto
            if is_valid and not validate_date(timestamp):
                is_valid = False
                stats["null_or_bad_timestamps"] += 1

            # Validación 4: Valores no nulos
            if is_valid and (
                temperature is None
                or humidity is None
                or co2 is None
                or battery is None
            ):
                is_valid = False
                stats["out_of_range_values"] += 1

            # Validación 5: Rangos válidos
            if is_valid:
                if not (-20 <= temperature <= 80):
                    is_valid = False
                elif not (0 <= humidity <= 100):
                    is_valid = False
                elif co2 <= 0:
                    is_valid = False
                elif not (0 <= battery <= 100):
                    is_valid = False

                if not is_valid:
                    stats["out_of_range_values"] += 1

            # Separar válidos e inválidos
            if is_valid:
                valid_data.append(record)
            else:
                invalid_data.append(record)

    # Actualizar contadores finales
    stats["valid_records"] = len(valid_data)
    stats["invalid_records"] = len(invalid_data) + stats["json_errors"]

    # Escribir válidos
    with open(VALID_FILE, "w", encoding="utf-8") as f:
        for rec in valid_data:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Escribir inválidos a cuarentena
    with open(INVALID_FILE, "w", encoding="utf-8") as f:
        for rec in invalid_data:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Escribir informe de calidad
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=4, ensure_ascii=False)

    # Resumen por consola
    print("✅ Validación de calidad completada")
    print(f"   Total registros      : {stats['total_records']}")
    print(f"   Registros válidos    : {stats['valid_records']}")
    print(f"   Registros inválidos  : {stats['invalid_records']}")
    print(f"   Duplicados           : {stats['duplicates']}")
    print(f"   Fechas erróneas      : {stats['null_or_bad_timestamps']}")
    print(f"   Fuera de rango       : {stats['out_of_range_values']}")
    print(f"   Faltan obligatorios  : {stats['missing_mandatory']}")
    print(f"   JSON corrupto        : {stats['json_errors']}")
    print(f"\n📄 Informe generado en: {REPORT_FILE}")
    print(f"🧪 Inválidos guardados en: {INVALID_FILE}")


if __name__ == "__main__":
    main()