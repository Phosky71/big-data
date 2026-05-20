"""
Publicación de datos Gold para Superset.

Flujo:
1. Lee la capa Gold desde MinIO (S3A) con Spark.
2. Publica/reemplaza la tabla analítica en PostgreSQL.
3. Se autentica contra la API de Superset.
4. Crea la conexión de base de datos en Superset si no existe.
5. Crea el dataset en Superset si no existe.

Requisitos en el contenedor Jupyter:
- requests
- sqlalchemy
- psycopg2-binary
"""

from __future__ import annotations

import json
import sys
from typing import Any, Dict, Optional

import pandas as pd
import requests
from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text

# -----------------------------------------------------------------------------
# Configuración del pipeline
# -----------------------------------------------------------------------------
GOLD_PATH = "s3a://datalake/gold/iot/sensor_aula_01/hourly_metrics/"

POSTGRES_SQLALCHEMY_URI = "postgresql+psycopg2://airflow:airflow@postgres/airflow"
POSTGRES_TABLE = "iot_hourly_metrics"
POSTGRES_SCHEMA = "public"

SUPERSET_BASE_URL = "http://superset:8088"
SUPERSET_USERNAME = "admin"
SUPERSET_PASSWORD = "admin"
SUPERSET_DB_NAME = "Airflow PostgreSQL"

# URI que Superset usará internamente para conectarse a PostgreSQL
SUPERSET_DATABASE_SQLALCHEMY_URI = "postgresql+psycopg2://airflow:airflow@postgres/airflow"

REQUEST_TIMEOUT = 30


# -----------------------------------------------------------------------------
# Spark / extracción Gold
# -----------------------------------------------------------------------------
def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IoT_Publish_Gold_Superset")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def load_gold_as_pandas(spark: SparkSession) -> pd.DataFrame:
    print(f"📂 Leyendo capa Gold: {GOLD_PATH}")
    df_gold = spark.read.parquet(GOLD_PATH).orderBy("date_hour")
    total = df_gold.count()
    print(f"📊 Total de registros Gold a publicar: {total}")
    pdf = df_gold.toPandas()

    if pdf.empty:
        raise RuntimeError("La capa Gold está vacía. No se puede publicar en Superset.")

    return pdf


# -----------------------------------------------------------------------------
# PostgreSQL publication
# -----------------------------------------------------------------------------
def publish_table_to_postgres(pdf: pd.DataFrame) -> None:
    print("🛢️ Publicando tabla en PostgreSQL...")
    engine = create_engine(POSTGRES_SQLALCHEMY_URI)

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}'))

    pdf.to_sql(
        POSTGRES_TABLE,
        engine,
        schema=POSTGRES_SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    print(f"✅ Tabla publicada: {POSTGRES_SCHEMA}.{POSTGRES_TABLE}")


# -----------------------------------------------------------------------------
# Superset API helpers
# -----------------------------------------------------------------------------
def raise_for_status(response: requests.Response, context: str) -> None:
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        body = response.text[:2000]
        raise RuntimeError(f"{context} falló: {exc}\nRespuesta: {body}") from exc


def create_superset_session() -> requests.Session:
    session = requests.Session()

    login_payload = {
        "username": SUPERSET_USERNAME,
        "password": SUPERSET_PASSWORD,
        "provider": "db",
        "refresh": True,
    }

    print("🔐 Autenticando en Superset...")
    response = session.post(
        f"{SUPERSET_BASE_URL}/api/v1/security/login",
        json=login_payload,
        timeout=REQUEST_TIMEOUT,
    )
    raise_for_status(response, "Login en Superset")

    data = response.json()
    access_token = data.get("access_token")
    if not access_token:
        raise RuntimeError("No se recibió access_token desde Superset.")

    session.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    })

    csrf_response = session.get(
        f"{SUPERSET_BASE_URL}/api/v1/security/csrf_token/",
        timeout=REQUEST_TIMEOUT,
    )
    raise_for_status(csrf_response, "Obtención de CSRF token")

    csrf_token = csrf_response.json().get("result")
    if csrf_token:
        session.headers.update({"X-CSRFToken": csrf_token})

    print("✅ Sesión autenticada en Superset")
    return session


def find_database_id(session: requests.Session, database_name: str) -> Optional[int]:
    response = session.get(
        f"{SUPERSET_BASE_URL}/api/v1/database/",
        params={"q": json.dumps({"filters": [{"col": "database_name", "opr": "eq", "value": database_name}]})},
        timeout=REQUEST_TIMEOUT,
    )
    raise_for_status(response, "Consulta de databases en Superset")

    result = response.json().get("result", [])
    if result:
        return result[0].get("id")
    return None


def create_database_in_superset(session: requests.Session) -> int:
    existing_id = find_database_id(session, SUPERSET_DB_NAME)
    if existing_id:
        print(f"✅ Database ya existente en Superset: {SUPERSET_DB_NAME} (id={existing_id})")
        return existing_id

    print("🧩 Creando database connection en Superset...")
    payload: Dict[str, Any] = {
        "database_name": SUPERSET_DB_NAME,
        "sqlalchemy_uri": SUPERSET_DATABASE_SQLALCHEMY_URI,
        "expose_in_sqllab": True,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_run_async": False,
    }

    response = session.post(
        f"{SUPERSET_BASE_URL}/api/v1/database/",
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    raise_for_status(response, "Creación de database en Superset")

    db_id = response.json().get("id")
    if not db_id:
        db_id = find_database_id(session, SUPERSET_DB_NAME)

    if not db_id:
        raise RuntimeError("No se pudo recuperar el id de la database creada en Superset.")

    print(f"✅ Database creada en Superset: {SUPERSET_DB_NAME} (id={db_id})")
    return int(db_id)


def find_dataset_id(session: requests.Session, table_name: str, schema: str, database_id: int) -> Optional[int]:
    response = session.get(
        f"{SUPERSET_BASE_URL}/api/v1/dataset/",
        params={
            "q": json.dumps({
                "filters": [
                    {"col": "table_name", "opr": "eq", "value": table_name},
                    {"col": "schema", "opr": "eq", "value": schema},
                    {"col": "database", "opr": "rel_o_m", "value": database_id},
                ]
            })
        },
        timeout=REQUEST_TIMEOUT,
    )
    raise_for_status(response, "Consulta de datasets en Superset")

    result = response.json().get("result", [])
    if result:
        return result[0].get("id")
    return None


def create_dataset_in_superset(session: requests.Session, database_id: int) -> int:
    existing_id = find_dataset_id(session, POSTGRES_TABLE, POSTGRES_SCHEMA, database_id)
    if existing_id:
        print(f"✅ Dataset ya existente en Superset: {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (id={existing_id})")
        return existing_id

    print("📚 Creando dataset en Superset...")
    payload = {
        "database": database_id,
        "schema": POSTGRES_SCHEMA,
        "table_name": POSTGRES_TABLE,
    }

    response = session.post(
        f"{SUPERSET_BASE_URL}/api/v1/dataset/",
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    raise_for_status(response, "Creación de dataset en Superset")

    result = response.json()
    dataset_id = result.get("id")
    if not dataset_id:
        dataset_id = find_dataset_id(session, POSTGRES_TABLE, POSTGRES_SCHEMA, database_id)

    if not dataset_id:
        raise RuntimeError("No se pudo recuperar el id del dataset creado en Superset.")

    print(f"✅ Dataset creado en Superset: {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (id={dataset_id})")
    return int(dataset_id)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    spark = None
    try:
        print("🚀 Iniciando publicación automática para Superset...")
        spark = build_spark()

        pdf = load_gold_as_pandas(spark)
        publish_table_to_postgres(pdf)

        session = create_superset_session()
        database_id = create_database_in_superset(session)
        dataset_id = create_dataset_in_superset(session, database_id)

        print("\n🎉 Publicación completada correctamente")
        print(f"   - Tabla PostgreSQL: {POSTGRES_SCHEMA}.{POSTGRES_TABLE}")
        print(f"   - Database Superset: {SUPERSET_DB_NAME} (id={database_id})")
        print(f"   - Dataset Superset: {POSTGRES_SCHEMA}.{POSTGRES_TABLE} (id={dataset_id})")
        print(f"   - Superset URL: {SUPERSET_BASE_URL}")

    except Exception as exc:
        print(f"❌ Error en publish_gold_superset.py: {exc}", file=sys.stderr)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()