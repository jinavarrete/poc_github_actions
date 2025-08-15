# Databricks notebook source

# COMMAND ----------

# Configuración del Job y conexión a fuentes externas
# VIOLACIÓN 1: Credenciales hardcodeadas. Esto es un riesgo de seguridad grave.
# La forma correcta es usar Databricks Secrets: dbutils.secrets.get(scope="...", key="...")

storage_account_access_key = "a_very_long_and_super_secret_access_key_12345"
api_token = "dapi123456789abcdefghijklmnopqrstuvwxyz"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VIOLACIÓN 2: Uso de 'SELECT *' que es ineficiente y poco explícito.
# MAGIC -- VIOLACIÓN 3: Referencia a un esquema de entorno ('prod.') que rompe la portabilidad del código.
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW bronze_data_with_dim AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   bronze.raw_events
# MAGIC JOIN
# MAGIC   prod.users_dimension ON raw_events.user_id = users_dimension.id;

# COMMAND ----------

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Leer los datos de la vista temporal creada en SQL
df_bronze = spark.table("bronze_data_with_dim")

# VIOLACIÓN 4: Definición de una UDF de Python. Es mucho más lento que usar funciones nativas de Spark.
# En este caso, se debería usar la función nativa `upper()`.
def to_upper_case(s):
  if s:
    return s.upper()
  return None

upper_udf = udf(to_upper_case, StringType())

df_with_udf = df_bronze.withColumn("name_upper", upper_udf(col("name")))

# COMMAND ----------

# VIOLACIÓN 5: Uso de .collect(). Esto es muy peligroso y puede causar un error de OutOfMemory (OOM)
# en el nodo driver si el DataFrame es grande.
print("Recolectando todos los datos en el driver... (Mala práctica)")
all_data_in_driver = df_with_udf.collect()
print(f"Se han recolectado {len(all_data_in_driver)} filas.")


# VIOLACIÓN 6: Uso de .toPandas() sin un .limit() previo. Causa el mismo problema que .collect().
print("Convirtiendo todo el DataFrame a Pandas... (Mala práctica)")
pandas_df = df_with_udf.toPandas()
print("Conversión a Pandas finalizada.")


# COMMAND ----------

# Forma correcta de inspeccionar datos sin riesgo de OOM
print("Inspección segura de datos:")
display(df_with_udf)

# O usando .show()
df_with_udf.show(10, truncate=False)

# O si necesitas una muestra en Pandas, limita los datos primero
print("Conversión segura a Pandas con límite:")
safe_pandas_df = df_with_udf.limit(100).toPandas()
print("Conversión segura finalizada.")