# Databricks notebook source
# MAGIC %run "/Workspace/trial/trial/Tutorial"

# COMMAND ----------

# MAGIC %md
# MAGIC # DELTA LAKE

# COMMAND ----------

df_sales.write.format('parquet')\
        .mode('append')\
        .option('path','abfss://destination@datalakegautam.dfs.core.windows.net/sales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed VS External Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Database**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE salesDB;

# COMMAND ----------

# MAGIC %md
# MAGIC **Managed Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesDB.mantable  
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT
# MAGIC )
# MAGIC USING DELTA  

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesDB.mantable 
# MAGIC VALUES
# MAGIC (1,'aa',30),
# MAGIC (2,'bb',33),
# MAGIC (3,'cc',35),
# MAGIC (4,'DD',40)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from salesDB.mantable;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE salesDB.mantable;

# COMMAND ----------

# MAGIC %md
# MAGIC **External Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE salesDB.exttable  
# MAGIC (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   marks INT 
# MAGIC )
# MAGIC USING DELTA    
# MAGIC LOCATION 'abfss://destination@datalakegautam.dfs.core.windows.net/salesDB/exttable' 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesDB.exttable 
# MAGIC VALUES
# MAGIC (1,'aa',30),
# MAGIC (2,'bb',33),
# MAGIC (3,'cc',35),
# MAGIC (4,'DD',40)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from salesDB.exttable;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Tables Functionalities

# COMMAND ----------

# MAGIC %md
# MAGIC **INSERT**

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO salesDB.exttable 
# MAGIC VALUES
# MAGIC (5,'aa',30),
# MAGIC (6,'bb',33),
# MAGIC (7,'cc',35),
# MAGIC (8,'DD',40)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdb.exttable

# COMMAND ----------

# MAGIC %md
# MAGIC **DELETE**

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM salesdb.exttable 
# MAGIC WHERE id = 8

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdb.exttable

# COMMAND ----------

# MAGIC %md
# MAGIC **DATA VERSIONING**

# COMMAND ----------

# MAGIC %md
# MAGIC **TIME TRAVEL**

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE salesdb.exttable TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesDB.exttable

# COMMAND ----------

# MAGIC %md
# MAGIC **VACUUM**

# COMMAND ----------

# MAGIC %md
# MAGIC **VACUUM RETAIN 0 HRS**

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM salesdb.exttable RETAIN 0 HOURS; 

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELTA Table Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC **OPTIMIZE**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdb.exttable

# COMMAND ----------

# MAGIC %md
# MAGIC **ZORDER BY**

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE salesdb.exttable ZORDER BY (id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from salesdb.exttable

# COMMAND ----------

# MAGIC %md
# MAGIC ### AUTO LOADER

# COMMAND ----------

# MAGIC %md
# MAGIC **Streaming Dataframe**

# COMMAND ----------

df = spark.readStream.format('cloudFiles')\
        .option('cloudFiles.format','parquet')\
        .option('cloudFiles.schemaLocation','abfss://aldestination@datalakegautam.dfs.core.windows.net/checkpoint')\
        .load('abfss://alsource@datalakegautam.dfs.core.windows.net')   

# COMMAND ----------

df.writeStream.format('delta')\
               .option('checkpointLocation','abfss://aldestination@datalakegautam.dfs.core.windows.net/checkpoint')\
               .option('mergeSchema','true')\
               .trigger(processingTime='5 seconds')\
               .start('abfss://aldestination@datalakegautam.dfs.core.windows.net/data')

# COMMAND ----------

