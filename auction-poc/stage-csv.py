# Databricks notebook source
# dbutils.widgets.removeAll()

# dbutils.fs.mkdirs("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc")
# dbutils.fs.mkdirs("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/landed_files")

# dbutils.fs.ls("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/landed_files")

# dbutils.widgets.text("src_file_directory","dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles")
# dbutils.widgets.text("src_file_name","Organizations.csv")
# dbutils.widgets.text("trg_schema_name","AUCTION_POC")
# dbutils.widgets.text("trg_table_name","STG_AUCTION_ORGANIZATION")

# spark.sql(f"drop table if exists {table_name}")

# COMMAND ----------

# %sql
# -- drop table auction_poc.stg_auction_organization;
# -- drop table auction_poc.stg_auction_instrument;
# drop table auction_poc.stg_auction_result;

# COMMAND ----------

# MAGIC %md
# MAGIC # Set Params

# COMMAND ----------


dbutils.widgets.text("job_run_id","")
dbutils.widgets.text("task_run_id","")
dbutils.widgets.text("src_file_directory","")
dbutils.widgets.text("src_file_name","")
dbutils.widgets.text("trg_schema_name","")
dbutils.widgets.text("trg_table_name","")



# COMMAND ----------

# MAGIC %md
# MAGIC # Create and use Schema

# COMMAND ----------


schema_name = dbutils.widgets.get('trg_schema_name')
spark.sql(f'create schema if not exists {schema_name}')
spark.sql(f'use schema {schema_name}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Create and load STG Table

# COMMAND ----------

# NOTE: We are not inferring a schema so everything will be a string.  If wanted, we could add this to FORMAT_OPTIONS 'inferSchema' = 'true'
table_name = dbutils.widgets.get('trg_table_name')
file_name = dbutils.widgets.get('src_file_name')
directory_name = dbutils.widgets.get('src_file_directory')
job_run_id = dbutils.widgets.get('job_run_id')
task_run_id = dbutils.widgets.get('task_run_id')
# spark.sql(f"drop table if exists {table_name}")
spark.sql(f"create table if not exists {table_name}")
#TODO change current_timestamp to GMT
spark.sql(f"""copy into {table_name} from 
          ( 
            select 
              uuid() as md_id, 
              *, 
              _metadata.file_name as md_file_name, 
              _metadata.file_modification_time as md_file_ts, 
              current_timestamp as md_audit_create_ts, 
              cast({job_run_id} as BIGINT) as md_job_run_id, 
              cast({task_run_id} as BIGINT) as md_task_run_id 
            from 
          '{directory_name}/{file_name}' 
          ) 
          FILEFORMAT = CSV 
          FORMAT_OPTIONS ('header' = 'true', 'mergeSchema' = 'true') 
          COPY_OPTIONS ('mergeSchema' = 'true')""")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Sample

# COMMAND ----------

df_sample = spark.sql(f"SELECT * FROM {table_name} order by md_audit_create_ts desc, md_file_ts desc limit 10")
print(table_name)
display(df_sample)
