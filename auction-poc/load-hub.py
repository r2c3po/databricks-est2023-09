# Databricks notebook source
# MAGIC %md
# MAGIC #Set Widgets

# COMMAND ----------

dbutils.widgets.text("src_lob_id","AUCTION")
dbutils.widgets.text("trg_schema_name","auction_poc")
dbutils.widgets.text("src_schema_name","auction_poc")
dbutils.widgets.text("mapping_directory","/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc")
dbutils.widgets.text("mapping_xl_name","AuctionDataMappings.xlsx")

dbutils.widgets.text("job_run_id","")
dbutils.widgets.text("task_run_id","")


# COMMAND ----------

# MAGIC %md
# MAGIC #Load Hub Tables for Line of Business (LOB_ID)

# COMMAND ----------

import pandas as pd
import datetime as dt
from pyspark.sql.types import*
from delta.tables import*


src_lob = dbutils.widgets.get("src_lob_id")
src_schema = dbutils.widgets.get("src_schema_name")
trg_schema = dbutils.widgets.get("trg_schema_name")
mapping_loc = dbutils.widgets.get("mapping_directory")
mapping_xl = dbutils.widgets.get("mapping_xl_name")

job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

# Reading Mapping
mapping =  pd.read_excel(mapping_loc + "/" + mapping_xl)
# print(mapping)

# Get list of TRG Tables by src_lob
table_list = (mapping.query('SRC_LOB == "' + src_lob + '" and TRG_TABLE_TYPE == "HUB"')[["TRG_TABLE"]]) \
             ["TRG_TABLE"].unique().tolist()


#!!!!!!!
#TODO Need to run these in parallel and modularize
#!!!!!!!

# For each table
for trg_table in table_list:
    print()
    print("Target Table: " + trg_table)
    root_name = trg_table.lstrip("H_")

    # Get Source Table
    src_table_list = (mapping.query(\
                     'SRC_LOB == "' + src_lob + '"' + ' and TRG_TABLE == "' + trg_table + '"  and TRG_TABLE_TYPE == "HUB"')\
                     [["SRC_TABLE"]])["SRC_TABLE"].unique().tolist()
    
    if len(src_table_list) == 1 :
        src_table = src_table_list[0]
        print("Source Table: " + src_table)

        # Get Mapping - HUBS only insert keys
        trg_to_src_df = mapping.sort_index().query(\
                        'SRC_LOB == "' + src_lob + '"' + ' and TRG_TABLE == "' + trg_table + '" and IS_BUSKEY == True and TRG_TABLE_TYPE == "HUB"')\
                        [["TRG_COL","SRC_COL"]]
        print(trg_table + " Mappings:")
        print(trg_to_src_df)

        # Source Columns
        src_cols_list =  trg_to_src_df["SRC_COL"].values.tolist()
        src_select_str = ', '.join(src_cols_list)

        # Source to Target Columns
        src2trg_cols_list = trg_to_src_df[["SRC_COL","TRG_COL"]].values.tolist()

        # Get list of bus keys in proper order
        src_keys_df = mapping.sort_values("BUSKEY_ORDER").query( 
                                                                'SRC_LOB == "' + src_lob + '"' + \
                                                                ' and TRG_TABLE == "' + trg_table + \
                                                                '" and IS_BUSKEY == True') \
                                                                ["SRC_COL"]
        src_keys_list = src_keys_df.values.tolist()
        print(trg_table + " SrcKeylist:")
        print(src_keys_list)
        src_key_cols = ', '.join(src_keys_list)

        # Source Keys with src prefix
        src_keys_scoped_list = ["src."+x for x in src_keys_list]
        src_keys_scoped_cols = ', '.join(src_keys_scoped_list)
  
        # HK column name
        hk_col = "HK_" + root_name + "_ID"

        # HK select clause with src_lob for multi-system uniqueness
        hk_select = "xxhash64(" + src_key_cols + ",'" + src_lob + "') as " + hk_col
        hk_src_value = "xxhash64(" + src_keys_scoped_cols + ",'" + src_lob + "')"

        # Where clause to filter out nulls
        src_cols_notnull_list = mapping.query( 
                                    'SRC_LOB == "' + src_lob + '"' + \
                                    ' and TRG_TABLE == "' + trg_table + \
                                    '" and TRG_NULLABLE == False') \
                                    ["SRC_COL"].values.tolist()

        src_cols_notnull_whereclause = ''
        if len(src_cols_notnull_list) > 0:
            src_cols_notnull_whereclause = ' where ' + ' is not null and '.join(src_cols_notnull_list) + ' is not null '

        # Build the target to source mapping dictionary
        mapping_dict = {hk_col : hk_src_value}
        for x in src2trg_cols_list:
            mapping_dict[x[1]] = "src." + x[0] 
        mapping_dict["LOB_ID"] = "'" + src_lob + "'"
        mapping_dict["MD_REC_SRC"] = "'" + src_table + "'"
        mapping_dict["MD_REC_SRC_ID"] = "src.MD_REC_SRC_ID"
        mapping_dict["MD_LOAD_DTS"] = '"' + dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + '"'
        mapping_dict["MD_JOB_RUN_ID"] = "'" + job_run_id + "'"
        mapping_dict["MD_TASK_RUN_ID"] = "'" + task_run_id + "'"
        print(trg_table + " Mapping Dictionary:")
        print(mapping_dict)

        # Select for the source data using mapping for aliasing the column names
        # NOTE the MD_REC_SRC_ID must take the first one, since it will break the uniquness of the key in teh case when there are two identical keys
        select_stmt = "with source as (" +\
                        "select distinct " + hk_select + ", " + src_select_str + \
                        " ," + "first(source.md_id) OVER (PARTITION BY " + src_key_cols + " ORDER BY md_audit_create_ts) as MD_REC_SRC_ID" +\
                        " from " + src_schema + "." + src_table + " as source " + src_cols_notnull_whereclause +\
                     ")" +\
                     "select * from source where NOT EXISTS " +\
                     "(select * from " + trg_schema + "." + trg_table + " as hub where hub."+ hk_col +" = source." + hk_col + ")"
        print()
        print("Source Select Statement: " + select_stmt)


        #Run Merge with insert only
        tgt_delta = DeltaTable.forName(spark,trg_schema + "." + trg_table)
        src_df = spark.sql(select_stmt)

        tgt_delta.alias('tgt').merge( 
            src_df.alias('src'),
            'tgt.' + hk_col + ' = src.' + hk_col 
            ) \
            .whenNotMatchedInsert(values = mapping_dict) \
            .execute()

