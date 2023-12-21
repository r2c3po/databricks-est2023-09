# Databricks notebook source
# MAGIC %md
# MAGIC #Set Widgets

# COMMAND ----------

dbutils.widgets.text("src_lob_id","AUCTION")
dbutils.widgets.text("trg_schema_name","auction_poc")
dbutils.widgets.text("mapping_directory","/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc")
dbutils.widgets.text("mapping_xl_name","AuctionDataMappings.xlsx")

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Hub Tables for Line of Business (LOB_ID)

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql.types import*
from delta.tables import*

src_lob = dbutils.widgets.get("src_lob_id")
trg_schema = dbutils.widgets.get("trg_schema_name")
mapping_loc = dbutils.widgets.get("mapping_directory")
mapping_xl = dbutils.widgets.get("mapping_xl_name")

# Reading Mapping
mapping =  pd.read_excel(mapping_loc + "/" + mapping_xl,dtype={'TRG_NULLABLE': np.bool_, 'IS_BUSKEY': np.bool_})

# Get list of TRG Tables by SRC_LOB
table_list = (mapping.query('SRC_LOB == "' + src_lob + '" and TRG_TABLE_TYPE == "HUB"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


# For each table
for trg_table in table_list:
    print()
    print("Target Table: " + trg_table)
    
    root_name = trg_table.lstrip("H_")
    print("Root Name: " + root_name)

    # Get list of bus keys in proper order
    keys_df = mapping.sort_values("BUSKEY_ORDER").query(
        'TRG_TABLE == "' + trg_table + \
        '" and IS_BUSKEY == True and TRG_TABLE_TYPE == "HUB"') \
        [["TRG_COL","TRG_TYPE","TRG_NULLABLE"]]
    keys_list = keys_df.values.tolist()
    print(trg_table + " Keylist:")
    print(keys_list)

    # create the schema for bus key columns - Only Keys used in HUBs
    cols=[]
    for x in keys_list:
        cols.append({'metadata':{},'name':x[0],'type':x[1],'nullable':x[2]})
    trg_schema_struct = StructType.fromJson({'fields':cols,'type':'struct'})
    print(trg_schema_struct)

    DeltaTable.createIfNotExists(spark) \
    .tableName(trg_schema + "." + trg_table) \
    .addColumn("HK_" + root_name + "_ID", "bigint") \
    .addColumns(trg_schema_struct) \
    .addColumn("LOB_ID", "string") \
    .addColumn("MD_REC_SRC", "string") \
    .addColumn("MD_REC_SRC_ID", "string") \
    .addColumn("MD_LOAD_DTS", "timestamp") \
    .addColumn("MD_JOB_RUN_ID", "bigint") \
    .addColumn("MD_TASK_RUN_ID", "bigint") \
    .execute()





# COMMAND ----------

display(mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC #Adhoc cleanup

# COMMAND ----------

# %sql
# drop table auction_poc.H_ORGANIZATION;
# drop table auction_poc.H_INSTRUMENT;

