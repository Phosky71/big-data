FROM apache/superset:3.1.2

# Cambiamos a root para instalar dependencias de sistema si hicieran falta
USER root

# Instalamos el motor de DuckDB para SQLAlchemy
RUN pip install --no-cache-dir \
    duckdb \
    duckdb-engine

# Volvemos al usuario de superset ANTES de instalar la extensión.
# DuckDB guarda las extensiones en ~/.duckdb/extensions/ del usuario que las instala.
# Si se instalan como root, Superset (que corre como 'superset') no las encuentra.
USER superset

# Pre-instalamos la extensión httpfs de DuckDB para el usuario 'superset'.
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs'); con.close()"
