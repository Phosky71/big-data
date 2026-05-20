"""
Carga de datos raw a HDFS Bronze vía WebHDFS REST API.

Bronze almacena únicamente el dato crudo sin transformar.
"""

import os
import requests

NAMENODE_HOST = "namenode"
NAMENODE_WEBHDFS_PORT = 9870
WEBHDFS_USER = "hadoop"
BASE_URL = f"http://{NAMENODE_HOST}:{NAMENODE_WEBHDFS_PORT}/webhdfs/v1"

RAW_FILE = "/opt/airflow/dags/data/raw/sensor_aula_01_raw.jsonl"

DATE_PARTITION = "year=2026/month=04/day=27"
HDFS_BRONZE_DIR = f"/datalake/bronze/iot/sensor_aula_01/{DATE_PARTITION}"


def check_local_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"No existe el fichero local requerido: {path}")


def webhdfs_mkdirs(hdfs_path):
    url = f"{BASE_URL}{hdfs_path}?op=MKDIRS&user.name={WEBHDFS_USER}"
    print(f"📁 Creando directorio HDFS: {hdfs_path}")
    r = requests.put(url)
    if not r.ok:
        raise RuntimeError(f"Error creando directorio HDFS {hdfs_path}: {r.status_code} {r.text}")
    print(f"   ✅ Directorio creado: {hdfs_path}")


def webhdfs_put(local_path, hdfs_dir):
    filename = os.path.basename(local_path)
    hdfs_path = f"{hdfs_dir}/{filename}"

    url_create = f"{BASE_URL}{hdfs_path}?op=CREATE&user.name={WEBHDFS_USER}&overwrite=true"
    print(f"📤 Subiendo fichero: {local_path} → {hdfs_path}")

    r = requests.put(url_create, allow_redirects=False)
    if r.status_code != 307:
        raise RuntimeError(
            f"NameNode no devolvió redirección al DataNode para {hdfs_path}: "
            f"status={r.status_code} body={r.text}"
        )

    datanode_url = r.headers["Location"]

    with open(local_path, "rb") as f:
        r2 = requests.put(datanode_url, data=f)

    if r2.status_code != 201:
        raise RuntimeError(
            f"Error subiendo fichero a DataNode {hdfs_path}: "
            f"status={r2.status_code} body={r2.text}"
        )

    print(f"   ✅ Fichero subido: {hdfs_path}")


def main():
    print("🚀 Iniciando carga a Bronze HDFS vía WebHDFS...")

    check_local_file(RAW_FILE)
    webhdfs_mkdirs(HDFS_BRONZE_DIR)
    webhdfs_put(RAW_FILE, HDFS_BRONZE_DIR)

    print("✅ Carga a Bronze HDFS completada con éxito.")
    print(f"📦 Ruta Bronze: {HDFS_BRONZE_DIR}")


if __name__ == "__main__":
    main()