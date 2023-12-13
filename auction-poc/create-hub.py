# Databricks notebook source
# MAGIC %md
# MAGIC #Set Widgets

# COMMAND ----------

dbutils.widgets.text("src_lob_id","AUCTION")
dbutils.widgets.text("trg_schema_name","auction_poc")
dbutils.widgets.text("mapping_directory","/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc")
dbutils.widgets.text("mapping_xl_name","TestDataMappings.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Hub Tables for Line of Business (LOB_ID)

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import*
from delta.tables import*

SRC_LOB = dbutils.widgets.get("src_lob_id")
TRG_SCHEMA_NAME = dbutils.widgets.get("trg_schema_name")
MAPPING_LOC = dbutils.widgets.get("mapping_directory")
MAPPING_XL_NAME = dbutils.widgets.get("mapping_xl_name")

# Reading Mapping
mapping =  pd.read_excel(MAPPING_LOC + "/" + MAPPING_XL_NAME)

# Get list of TRG Tables by SRC_LOB
tablelist = (mapping.query('SRC_LOB == "' + SRC_LOB + '"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


# For each table
for trgtable in tablelist:
    print()
    print("Target Table: " + trgtable)
    
    root_name = trgtable.lstrip("H_")
    print("Root Name: " + root_name)

    # Get list of bus keys in proper order
    keysdf = mapping.sort_values("BUSKEY_ORDER").query('TRG_TABLE == "' + trgtable + '" and IS_BUSKEY == True')[["TRG_COL","TRG_TYPE","TRG_NULLABLE"]]
    keyslist = keysdf.values.tolist()
    print(trgtable + " Keylist:")
    print(keyslist)

    # create the schema for bus key columns - Only Keys used in HUBs
    cols=[]
    for x in keyslist:
        cols.append({'metadata':{},'name':x[0],'type':x[1],'nullable':x[2]})
    trg_schema = StructType.fromJson({'fields':cols,'type':'struct'})
    print(trg_schema)

    DeltaTable.createIfNotExists(spark) \
    .tableName(TRG_SCHEMA_NAME + "." + trgtable) \
    .addColumn("HK_" + root_name + "_ID", "bigint") \
    .addColumns(trg_schema) \
    .addColumn("LOB_ID", "string") \
    .addColumn("MD_REC_SRC", "string") \
    .addColumn("MD_REC_SRC_ID", "bigint") \
    .addColumn("MD_LOAD_DTS", "timestamp") \
    .addColumn("MD_JOB_RUN_ID", "bigint") \
    .addColumn("MD_TASK_RUN_ID", "bigint") \
    .execute()





# COMMAND ----------

# MAGIC %md
# MAGIC #Adhoc cleanup

# COMMAND ----------

# %sql
# drop table auction_poc.H_ORGANIZATION;
# drop table auction_poc.H_INSTRUMENT;
# drop table auction_poc.TEST;
