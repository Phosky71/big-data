from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "alumno",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 27),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="iot_medallion_pipeline",
    default_args=default_args,
    description="Pipeline IoT con arquitectura Medallón",
    schedule_interval=None,
    catchup=False,
    tags=["iot", "medallion", "bigdata"],
) as dag:

    AIRFLOW_SCRIPTS = "/opt/airflow/dags/jobs"
    JUPYTER_SCRIPTS = "/home/jovyan/work/src/jobs"
    SPARK_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

    generar_datos = BashOperator(
        task_id="generar_datos_iot",
        bash_command=f"python {AIRFLOW_SCRIPTS}/generate_iot_data.py",
    )

    validar_raw = BashOperator(
        task_id="validar_calidad_raw",
        bash_command=f"python {AIRFLOW_SCRIPTS}/validate_raw.py",
    )

    cargar_bronze = BashOperator(
        task_id="cargar_bronze_hdfs",
        bash_command=f"python {AIRFLOW_SCRIPTS}/load_bronze_hdfs.py",
    )

    transformar_bronze_a_silver = BashOperator(
        task_id="transformar_bronze_a_silver",
        bash_command=(
            "docker exec jupyter-aula "
            f"spark-submit --packages {SPARK_PACKAGES} "
            f"{JUPYTER_SCRIPTS}/bronze_to_silver.py"
        ),
    )

    validar_silver = BashOperator(
        task_id="validar_silver",
        bash_command=(
            "docker exec jupyter-aula "
            f"spark-submit --packages {SPARK_PACKAGES} "
            f"{JUPYTER_SCRIPTS}/validate_silver.py"
        ),
    )

    transformar_silver_a_gold = BashOperator(
        task_id="transformar_silver_a_gold",
        bash_command=(
            "docker exec jupyter-aula "
            f"spark-submit --packages {SPARK_PACKAGES} "
            f"{JUPYTER_SCRIPTS}/silver_to_gold.py"
        ),
    )

    publicar_gold_para_superset = BashOperator(
        task_id="publicar_gold_para_superset",
        bash_command=(
            "docker exec jupyter-aula "
            f"spark-submit --packages {SPARK_PACKAGES} "
            f"{JUPYTER_SCRIPTS}/publish_gold_superset.py"
        ),
    )

    (
        generar_datos
        >> validar_raw
        >> cargar_bronze
        >> transformar_bronze_a_silver
        >> validar_silver
        >> transformar_silver_a_gold
        >> publicar_gold_para_superset
    )