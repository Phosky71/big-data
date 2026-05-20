"""
Validación de calidad de datos IoT.
Lee el JSONL raw, aplica reglas de calidad y separa los datos en válidos e inválidos.
Genera un informe de calidad.
"""

import json
import os
from datetime import datetime

# Rutas alineadas con tu generador
INPUT_FILE = "/opt/airflow/src/data/raw/sensor_aula_01_raw.jsonl"
OUTPUT_DIR_VALID = "/opt/airflow/src/data/valid"
OUTPUT_DIR_QUARANTINE = "/opt/airflow/src/data/quarantine"

os.makedirs(OUTPUT_DIR_VALID, exist_ok=True)
os.makedirs(OUTPUT_DIR_QUARANTINE, exist_ok=True)

VALID_FILE = os.path.join(OUTPUT_DIR_VALID, "sensor_aula_01_valid.jsonl")
INVALID_FILE = os.path.join(OUTPUT_DIR_QUARANTINE, "sensor_aula_01_invalid.jsonl")
REPORT_FILE = os.path.join(OUTPUT_DIR_QUARANTINE, "quality_report.json")

def validate_date(date_text):
    try:
        datetime.strptime(date_text, "%Y-%m-%dT%H:%M:%S")
        return True
    except (ValueError, TypeError):
        return False

def main():
    stats = {
        "total_records": 0,
        "valid_records": 0,
        "invalid_records": 0,
        "duplicates": 0,
        "null_or_bad_timestamps": 0,
        "out_of_range_values": 0,
        "missing_mandatory": 0
    }
    
    seen_events = set()
    valid_data = []
    invalid_data = []

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            stats["total_records"] += 1
            record = json.loads(line.strip())
            is_valid = True
            
            # 1. Columnas obligatorias
            if not record.get('event_id') or not record.get('device_id'):
                is_valid = False
                stats["missing_mandatory"] += 1

            # 2. Control de duplicados
            elif record['event_id'] in seen_events:
                is_valid = False
                stats["duplicates"] += 1
            else:
                seen_events.add(record['event_id'])

            # 3. Fechas erróneas o vacías
            if is_valid and not validate_date(record.get('timestamp')):
                is_valid = False
                stats["null_or_bad_timestamps"] += 1

            # 4. Control de nulos en métricas críticas
            if is_valid and record.get('temperature') is None:
                is_valid = False
                stats["out_of_range_values"] += 1

            # 5. Rangos y valores coherentes
            if is_valid:
                temp = record.get('temperature', 0)
                hum = record.get('humidity', 0)
                co2 = record.get('co2', 0)
                bat = record.get('battery', 0)

                if not (-20 <= temp <= 80) or not (0 <= hum <= 100) or (co2 <= 0) or not (0 <= bat <= 100):
                    is_valid = False
                    stats["out_of_range_values"] += 1

            # Clasificar
            if is_valid:
                valid_data.append(record)
            else:
                invalid_data.append(record)

    stats["valid_records"] = len(valid_data)
    stats["invalid_records"] = len(invalid_data)

    # Escritura de resultados
    with open(VALID_FILE, 'w', encoding='utf-8') as f:
        for rec in valid_data:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')
            
    with open(INVALID_FILE, 'w', encoding='utf-8') as f:
        for rec in invalid_data:
            f.write(json.dumps(rec, ensure_ascii=False) + '\n')

    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=4, ensure_ascii=False)

    print("✅ Validación completada:")
    print(f"   └─ Totales: {stats['total_records']} | Válidos: {stats['valid_records']} | Inválidos: {stats['invalid_records']}")
    print(f"   └─ Reporte y datos sucios guardados en: {OUTPUT_DIR_QUARANTINE}")

if __name__ == "__main__":
    main()