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
# MAGIC #Create DIM view for every HUB table in this Line of Business (LOB_ID)

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
hub_list = (mapping.query('SRC_LOB == "' + src_lob + '" and TRG_TABLE_TYPE == "HUB"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


# For each table
for hub_table in hub_list:
    print()
    print("For Hub Table: " + hub_table)
    
    root_name = hub_table.lstrip("H_")
    print("Root Name: " + root_name)
    
    skey_select = f"HK_{root_name}_ID as {root_name}_SKEY"

    # Get list of bus keys in proper order
    keys_col_list = mapping.sort_values("BUSKEY_ORDER").query(
        'TRG_TABLE == "' + hub_table + \
        '" and IS_BUSKEY == True and TRG_TABLE_TYPE == "HUB"') \
        ["TRG_COL"].values.tolist()

    # Source Keys with src prefix
    keys_col_list_scoped = ["H."+x for x in keys_col_list]
    key_col_select = ', '.join(keys_col_list_scoped)

    # loop through all SAT tables where HUB_REF_TABLE = hub_table
    sat_list = (mapping.sort_index().query(f'SRC_LOB == "{src_lob}" and TRG_TABLE_TYPE == "SAT" and HUB_REF_TABLE == "{hub_table}"')
                [["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()
    
    for sat_table in sat_list:
        # get all the non-key columns
        sat_col_list = mapping.sort_index().query(
            'TRG_TABLE == "' + sat_table + \
            '" and IS_BUSKEY == False and TRG_TABLE_TYPE == "SAT"') \
            ["TRG_COL"].values.tolist()
        
        # sat col with src prefix 
        sat_col_list_scoped = [sat_table+"."+x for x in keys_col_list]
        sat_col_select = ', '.join(sat_col_list_scoped)

        # append to sat_all_col_select
        #TODO



    # Build select statement
    select_str = f"""
                SELECT
                {skey_select},
                {key_col_select}
                FROM {trg_schema}.{hub_table} H
                """

    
    print(select_str)

    # # create the schema for bus key columns - Only Keys used in HUBs
    # cols=[]
    # for x in keys_list:
    #     cols.append({'metadata':{},'name':x[0],'type':x[1],'nullable':x[2]})
    # trg_schema_struct = StructType.fromJson({'fields':cols,'type':'struct'})
    # print(trg_schema_struct)

    # DeltaTable.createIfNotExists(spark) \
    # .tableName(trg_schema + "." + trg_table) \
    # .addColumn("HK_" + root_name + "_ID", "bigint") \
    # .addColumns(trg_schema_struct) \
    # .addColumn("LOB_ID", "string") \
    # .addColumn("MD_REC_SRC", "string") \
    # .addColumn("MD_REC_SRC_ID", "string") \
    # .addColumn("MD_LOAD_DTS", "timestamp") \
    # .addColumn("MD_JOB_RUN_ID", "bigint") \
    # .addColumn("MD_TASK_RUN_ID", "bigint") \
    # .execute()





# COMMAND ----------

display(mapping)

# COMMAND ----------

# MAGIC %md
# MAGIC #Adhoc cleanup

# COMMAND ----------

# %sql
# drop table auction_poc.H_ORGANIZATION;
# drop table auction_poc.H_INSTRUMENT;

