# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC with source as (select distinct xxhash64(srccol1, srccol2) as HK_ORGANIZATION_ID, srccol1 as ABC, srccol2 as XYZ ,'AUCTION' as LOB_ID ,'STG_TEST_ORGANIZATION' as MD_REC_SRC ,
# MAGIC -- source.md_id as MD_REC_SRC_ID ,
# MAGIC
# MAGIC first(source.md_id) OVER (PARTITION BY srccol1, srccol2 ORDER BY md_audit_create_ts) as MD_REC_SRC_ID ,
# MAGIC
# MAGIC '2023-12-14 20:27:33' as MD_LOAD_DTS ,1 as MD_JOB_RUN_ID ,1 as MD_TASK_RUN_ID from auction_poc.STG_TEST_ORGANIZATION as source)select * from source where NOT EXISTS (select * from auction_poc.H_ORGANIZATION as hub where hub.HK_ORGANIZATION_ID = source.HK_ORGANIZATION_ID)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.h_instrument

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.h_organization

# COMMAND ----------

# %sh
# cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc
# # rm TestDataMappings.xlsx
# ls
