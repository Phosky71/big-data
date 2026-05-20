FROM jupyter/pyspark-notebook:python-3.11.6

USER root

# ---------------------------------------------------------------------------
# Instalar Cliente Hadoop (HDFS/YARN CLI)
# ---------------------------------------------------------------------------
# Necesitamos 'curl' para descargar. 'apt-get' está disponible en imágenes basadas en Ubuntu/Debian.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Descargar e instalar Hadoop 3.4.0 (mismo que el cluster)
ENV HADOOP_VERSION=3.4.0
ENV HADOOP_URL=https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
ENV HADOOP_HOME=/opt/hadoop


RUN curl --retry 5 --retry-delay 3 -fL ${HADOOP_URL} -o /tmp/hadoop.tar.gz && \
    tar -xvf /tmp/hadoop.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz && \
    # Crear directorios para evitar warnings y asignar permisos
    mkdir -p ${HADOOP_HOME}/logs && \
    chown -R ${NB_UID}:${NB_GID} ${HADOOP_HOME}

# Añadir binarios de Hadoop al PATH
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}
# Configurar HADOOP_CONF_DIR (donde buscaremos los .xml montados por docker-compose)
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ---------------------------------------------------------------------------
# Instalar Librerías Python Adicionales
# ---------------------------------------------------------------------------
# IMPORTANTE: boto3 y s3fs se instalan JUNTOS en el mismo paso --upgrade.
# Motivo: s3fs depende de aiobotocore, que a su vez requiere una versión de
# botocore ligeramente distinta a la que boto3 pide. Si los instalamos por
# separado, pip resuelve cada uno de forma independiente y genera un conflicto
# de botocore. Instalándolos en el mismo comando, el resolver de pip encuentra
# el conjunto de versiones mutuamente compatible en un solo paso.
#
# s3fs y fsspec se pasan con --upgrade para sobreescribir las versiones ANTIGUAS
# que la imagen base (jupyter/pyspark-notebook) instala vía conda. Sin --upgrade,
# fsspec detecta la vieja y lanza un UserWarning de rendimiento grave.
# PyArrow, Pandas y Matplotlib ya vienen incluidos en la imagen base pyspark-notebook.
# Instalamos aquí solo las librerías adicionales necesarias para los laboratorios.
#
# IMPORTANTE - Todo en un único pip install --upgrade:
# La imagen base tiene versiones antiguas de s3fs/fsspec instaladas vía conda.
# boto3 y s3fs comparten la dependencia botocore a través de aiobotocore y deben
# resolverse juntos. Un único paso --upgrade permite al resolver de pip encontrar
# el conjunto de versiones mutuamente compatible de una sola vez.
RUN pip install --no-cache-dir --upgrade \
    hdfs \
    polars \
    "psycopg2-binary>=2.9.9" \
    "SQLAlchemy>=2.0.0" \
    duckdb \
    boto3 \
    s3fs \
    fsspec \
	requests \
    && fix-permissions "${CONDA_DIR}" \
    && fix-permissions "/home/${NB_USER}"

# Cambiar al usuario notebook (no root) para ejecución
USER ${NB_UID}
