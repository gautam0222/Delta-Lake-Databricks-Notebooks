# Databricks notebook source
# MAGIC %md
# MAGIC # DATABRICKS MASTERCLASS

# COMMAND ----------

mydata = [(1,'aa',30),(2,'bb',40),(3,'cc',50)]

myschema = "id INT, name STRING, marks INT"

df = spark.createDataFrame(mydata,schema=myschema)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Data

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="<scope>",key="<service-credential-key>")

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<application-id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DB Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.fs()**

# COMMAND ----------

dbutils.fs.ls("abfss://source@datalakegautam.dfs.core.windows.net/")

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.widgets**

# COMMAND ----------

dbutils.widgets.text("p_name","gautam")

# COMMAND ----------

var = dbutils.widgets.get("p_name")

# COMMAND ----------

# MAGIC %md
# MAGIC **dbutils.secrets**

# COMMAND ----------

dbutils.secrets.list(scope='gautamscope')

# COMMAND ----------

dbutils.secrets.get(scope='gautamscope',key='app-secret')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df_sales = spark.read.format('csv')\
              .option('header',True)\
              .option('inferSchema',True)\
              .load('abfss://source@datalakegautam.dfs.core.windows.net/') 


# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_sales.withColumn('Item_Type',split(col('Item_Type'),' ')).display()

# COMMAND ----------

df_sales.withColumn('flag',lit(var)).display()

# COMMAND ----------

df_sales.withColumn('Item_Visibility',col('Item_Visibility').cast(StringType())).display()

# COMMAND ----------

