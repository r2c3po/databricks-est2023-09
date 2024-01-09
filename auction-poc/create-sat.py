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
# MAGIC #Create Satellite Tables for Line of Business (LOB_ID)

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import*
from delta.tables import*

src_lob = dbutils.widgets.get("src_lob_id")
trg_schema = dbutils.widgets.get("trg_schema_name")
mapping_loc = dbutils.widgets.get("mapping_directory")
mapping_xl = dbutils.widgets.get("mapping_xl_name")

# Reading Mapping
mapping =  pd.read_excel(mapping_loc + "/" + mapping_xl)

# Get list of TRG Tables by SRC_LOB
table_list = (mapping.query('SRC_LOB == "' + src_lob + '" and TRG_TABLE_TYPE == "SAT"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


# For each table
for trg_table in table_list:
    print()
    print("Target Table: " + trg_table)
    
    

    # Get HUB_REF_TABLE - should only be one
    hub_ref_tbl_nm = (mapping.query(
        'TRG_TABLE == "' + trg_table + \
        '" and TRG_TABLE_TYPE == "SAT"') \
            [["HUB_REF_TABLE"]])["HUB_REF_TABLE"].unique().tolist()[0]
    
    hk_root_name = hub_ref_tbl_nm.lstrip("H_")
    print("HK Root Name: " + hk_root_name)

    # Get list of ALL columns in proper order
    #TODO need to change name from BUSKEY_ORDER to COL_ORDER
    cols_df = mapping.sort_values("BUSKEY_ORDER").query(
        'TRG_TABLE == "' + trg_table + \
        '" and TRG_TABLE_TYPE == "SAT"') \
        [["TRG_COL","TRG_TYPE","TRG_NULLABLE"]]
    cols_list = cols_df.values.tolist()
    print(trg_table + " Keylist:")
    print(cols_list)

    # create the schema for bus key columns - Only Keys used in HUBs
    cols=[]
    for x in cols_list:
        cols.append({'metadata':{},'name':x[0],'type':x[1],'nullable':x[2]})
    trg_schema_struct = StructType.fromJson({'fields':cols,'type':'struct'})
    print(trg_schema_struct)

    DeltaTable.createIfNotExists(spark) \
    .tableName(trg_schema + "." + trg_table) \
    .addColumn("HK_" + hk_root_name + "_ID", "bigint") \
    .addColumns(trg_schema_struct) \
    .addColumn("HK_COMPARE", "bigint") \
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
# drop table auction_poc.S_ORGANIZATION;
# drop table auction_poc.S_INSTRUMENT;
# drop table hive_metastore.auction_poc.s_auction;
# drop table hive_metastore.auction_poc.s_bid;
# drop table hive_metastore.auction_poc.s_bid_allot;

