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
# MAGIC #Create Link Tables for Line of Business (LOB_ID)

# COMMAND ----------

import pandas as pd
import re
from pyspark.sql.types import*
from delta.tables import*

src_lob = dbutils.widgets.get("src_lob_id")
trg_schema = dbutils.widgets.get("trg_schema_name")
mapping_loc = dbutils.widgets.get("mapping_directory")
mapping_xl = dbutils.widgets.get("mapping_xl_name")

# Reading Mapping
mapping =  pd.read_excel(mapping_loc + "/" + mapping_xl)

# Get list of TRG Tables by SRC_LOB
table_list = (mapping.query('SRC_LOB == "' + src_lob + '" and TRG_TABLE_TYPE == "LINK"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()



# For each table
for trg_table in table_list:
    print()
    print("Target Table: " + trg_table)
    
    root_name = trg_table.lstrip("L_")
    print("Root Name: " + root_name)

    # Get the two tables that are being linked
    linktbls_list = mapping.sort_values("LINK_KEY_ORDER").query(
        'TRG_TABLE == "' + trg_table + \
        '" and TRG_TABLE_TYPE == "LINK"') \
        ["LINK_REF_TABLE"].tolist()

    # Links are only made bewteen HUBs so we assume to remove "H_"
    ref_1_root_name = linktbls_list[0].lstrip("H_")
    ref_2_root_name = linktbls_list[1].lstrip("H_")

    DeltaTable.createIfNotExists(spark) \
    .tableName(trg_schema + "." + trg_table) \
    .addColumn("HK_" + root_name + "_ID", "bigint") \
    .addColumn("HK_" + ref_1_root_name + "_ID", "bigint") \
    .addColumn("HK_" + ref_2_root_name + "_ID", "bigint") \
    .addColumn("LOB_ID", "string") \
    .addColumn("MD_REC_SRC", "string") \
    .addColumn("MD_REC_SRC_ID", "string") \
    .addColumn("MD_LOAD_DTS", "timestamp") \
    .addColumn("MD_JOB_RUN_ID", "bigint") \
    .addColumn("MD_TASK_RUN_ID", "bigint") \
    .execute()





# COMMAND ----------

# MAGIC %md
# MAGIC #Adhoc cleanup

# COMMAND ----------

# %sql
# drop table auction_poc.L_ELIGIBILITY;


