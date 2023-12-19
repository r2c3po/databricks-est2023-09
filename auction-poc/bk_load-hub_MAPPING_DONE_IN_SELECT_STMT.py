# Databricks notebook source
# MAGIC %md
# MAGIC #Set Widgets

# COMMAND ----------

dbutils.widgets.text("src_lob_id","AUCTION")
dbutils.widgets.text("trg_schema_name","auction_poc")
dbutils.widgets.text("src_schema_name","auction_poc")
dbutils.widgets.text("mapping_directory","/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc")
dbutils.widgets.text("mapping_xl_name","TestDataMappings.xlsx")

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


SRC_LOB = dbutils.widgets.get("src_lob_id")
SRC_SCHEMA_NAME = dbutils.widgets.get("src_schema_name")
TRG_SCHEMA_NAME = dbutils.widgets.get("trg_schema_name")
MAPPING_LOC = dbutils.widgets.get("mapping_directory")
MAPPING_XL_NAME = dbutils.widgets.get("mapping_xl_name")

job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")

# Reading Mapping
mapping =  pd.read_excel(MAPPING_LOC + "/" + MAPPING_XL_NAME)
# print(mapping)

# Get list of TRG Tables by SRC_LOB
tablelist = (mapping.query('SRC_LOB == "' + SRC_LOB + '"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


#!!!!!!!
#TODO Need to run these in parallel and modularize
#!!!!!!!

# For each table
for trgtable in tablelist:
    print()
    print("Target Table: " + trgtable)
    root_name = trgtable.lstrip("H_")

    # Get Source Table
    srctablelist = (mapping.query(\
        'SRC_LOB == "' + SRC_LOB + '"' + ' and TRG_TABLE == "' + trgtable + '"')\
            [["SRC_TABLE"]])["SRC_TABLE"].unique().tolist()
    if len(srctablelist) == 1 :
        srctable = srctablelist[0]
        print("Source Table: " + srctable)

        # Get Mapping - HUBS only insert keys
        trg2srcdf = mapping.sort_index().query(\
            'SRC_LOB == "' + SRC_LOB + '"' + ' and TRG_TABLE == "' + trgtable + '" and IS_BUSKEY == True')\
                [["TRG_COL","SRC_COL"]]
        print(trgtable + " Mappings:")
        print(trg2srcdf)

        # Target Columns
        trgcolslist = trg2srcdf["TRG_COL"].values.tolist()
        print(trgcolslist)

        # Source to Target Columns
        src2trgcolslist = trg2srcdf[["SRC_COL","TRG_COL"]].values.tolist()
        print("src2trgcolslist")
        print(src2trgcolslist)

        # Build up the select string with Source to Target columns
        src2trgselectlist = [x[0] + " as " + x[1] for x in src2trgcolslist]
        print("src2trgselectlist")
        print(src2trgselectlist)
        src2trgselectstr = ', '.join(src2trgselectlist)
        print("src2trgselectstr")
        print(src2trgselectstr)

        # Get list of bus keys in proper order
        srckeysdf = mapping.sort_values("BUSKEY_ORDER").query('SRC_LOB == "' + SRC_LOB + '"' + ' and TRG_TABLE == "' + trgtable + '" and IS_BUSKEY == True')["SRC_COL"]
        srckeyslist = srckeysdf.values.tolist()
        print(trgtable + " SrcKeylist:")
        print(srckeyslist)
        srckeycols = ', '.join(srckeyslist)
  

        # HK column name
        hkcol = "HK_" + root_name + "_ID"

        # HK select clause with SRC_LOB for multi-system uniqueness
        hkselect = "xxhash64(" + srckeycols + ",'" + SRC_LOB + "') as " + hkcol
        print("HK select clause: " + hkselect)

        # Select for the source data using mapping for aliasing the column names
        # NOTE the MD_REC_SRC_ID must take the first one, since it will break the uniquness of the key in teh case when there are two identical keys
        selectstmt = "with source as (" +\
                        "select distinct " + hkselect + ", " + src2trgselectstr + \
                        " ,'" + SRC_LOB + "' as LOB_ID" +\
                        " ,'" + srctable + "' as MD_REC_SRC" +\
                        " ," + "first(source.md_id) OVER (PARTITION BY " + srckeycols + " ORDER BY md_audit_create_ts) as MD_REC_SRC_ID" +\
                        " ,'" + dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "' as MD_LOAD_DTS" +\
                        " ," + job_run_id + " as MD_JOB_RUN_ID" +\
                        " ," + task_run_id + " as MD_TASK_RUN_ID" +\
                        " from " + SRC_SCHEMA_NAME + "." + srctable + " as source"  +\
                     ")" +\
                     "select * from source where NOT EXISTS " +\
                     "(select * from " + TRG_SCHEMA_NAME + "." + trgtable + " as hub where hub."+ hkcol +" = source." + hkcol + ")"
        print()
        print("Source Select Statement: " + selectstmt)


        #Run Merge with insert only

        tgtdelta = DeltaTable.forName(spark,TRG_SCHEMA_NAME + "." + trgtable)
        srcdf = spark.sql(selectstmt)

        tgtdelta.alias('tgt') \
            .merge(srcdf.alias('src'),
            'tgt.' + hkcol + ' = src.' + hkcol 
            ) \
            .whenNotMatchedInsertAll() \
            .execute()



        

        

     


# COMMAND ----------


        # dictMap = {
        #     'HK_INSTRUMENT_ID': "xxhash64(src.BLAH1,'AUCTION')",
        #     'BLAH1': 'src.BLAH1',
        #     'LOB_ID': "'AUCTION'",
        #     'MD_REC_SRC': "'STG_TEST_ORGANIZATION'",
        #     'MD_REC_SRC_ID': "1234",
        #     'MD_LOAD_DTS': "'2023-12-18 17:36:56'" ,
        #     "MD_JOB_RUN_ID" : "1",
        #     "MD_TASK_RUN_ID" : "1"
        #     }


        # tgtdelta.alias('tgt') \
        #     .merge(srcdf.alias('src'),
        #     'tgt.' + hkcol + ' = src.' + hkcol 
        #     ) \
        #     .whenNotMatchedInsert(values = dictMap) \
        #     .execute()
