# Databricks notebook source
dbutils.widgets.text("src_schema_name","AUCTION_POC")
dbutils.widgets.text("trg_schema_name","AUCTION_POC")
dbutils.widgets.text("src_table_name","STG_AUCTION_ORGANIZATION")
dbutils.widgets.text("trg_table_name","H_ORGANIZATION")
dbutils.widgets.text("src_buskey_cols","ParticipantId")
dbutils.widgets.text("src_lob_id","AUCTION")

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/
# MAGIC ls
# MAGIC # rm AuctionDataMappings-1.xlsx
# MAGIC

# COMMAND ----------


# # Read the Excel file into a DataFrame
# excel_df = spark.read.format("com.crealytics.spark.excel") \
# .option("header", "true") \
# .load("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/AuctionDataMappings.xlsx") # Update with your file path


# #dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/AuctionDataMappings.xlsx

import pandas as pd
pd.read_excel("/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/TestDataMappings.xlsx")



# COMMAND ----------

import pandas as pd
from pyspark.sql.types import*
from delta.tables import*

SRC_LOB = "AUCTION"
MAPPING_LOC = "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc"
MAPPING_XL_NAME = "TestDataMappings.xlsx"
TRG_SCHEMA_NAME = "AUCTION_POC"
TRG_TBL_NAME = "TEST"

# Reading Mapping
mapping =  pd.read_excel(MAPPING_LOC + "/" + MAPPING_XL_NAME)

# Get list of TRG Tables by SRC_LOB
tablelist = (mapping.query('SRC_LOB == "AUCTION"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


# For each table
for trgtable in tablelist:
    print("Target Table: " + trgtable)
    
    # TRG_TBL_NAME = dbutils.widgets.get("trg_table_name")
    root_name = trgtable.lstrip("H_")
    print(root_name)

# SRC_BUSKEY_COLS = dbutils.widgets.get("src_buskey_cols")
# src_buskey_list = [x.strip() for x in SRC_BUSKEY_COLS.split(",")]
# print(src_buskey_list)

# head = ["TRG_COL", "TRG_TYPE", "SRC_COL", "IS_BUSKEY", "BUSKEY_ORDER"]
# mappingdata = [("ABC","integer","srccol1",True,1),("XYZ","string","srccol2",True,2),("MNO","string","srccol3",False,)]
# mapping = pd.DataFrame(mappingdata, columns=head)


print(mapping)

# Get list of bus keys in proper order
keysdf = mapping.sort_values("BUSKEY_ORDER").query('IS_BUSKEY == True')[["TRG_COL","TRG_TYPE"]]
keyslist = keysdf.values.tolist()
print()
print(keyslist)
print()

# create the schema for bus key columns
cols=[]
for x in keyslist:
    cols.append({'metadata':{},'name':x[0],'type':x[1],'nullable':True})
trg_schema = StructType.fromJson({'fields':cols,'type':'struct'})

print ({'fields':cols,'type':'struct'})

# print (schema)

DeltaTable.createIfNotExists(spark) \
  .tableName(TRG_SCHEMA_NAME + "." + TRG_TBL_NAME) \
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

from delta.tables import*


job_run_id = dbutils.widgets.get('job_run_id')
task_run_id = dbutils.widgets.get('task_run_id')


SRC_SCHEMA_NAME = dbutils.widgets.get("src_schema_name")
TRG_SCHEMA_NAME = dbutils.widgets.get("trg_schema_name")
SRC_TBL_NAME = dbutils.widgets.get("src_table_name")
TRG_TBL_NAME = dbutils.widgets.get("trg_table_name")
SRC_BUSKEY_COLS = dbutils.widgets.get("src_buskey_cols")
SRC_LOB = dbutils.widgets.get("src_lob_id")


root_name = TRG_TBL_NAME.lstrip("H_")



# src_df = spark.read.format("delta") \
#     .table(SCHEMA_NAME + "." + SRC_TBL_NAME)

# # get all the columns that do not start with md_ (exclude metadata columns)
# src_cols = src_df.select([column for column in src_df.columns if not(column.startswith("md_"))])

# # schema = StructType([StructField('SignalType', StringType()),StructField('StartTime', TimestampType())])

# trg bus key
trg_buskey_list = list(SRC_BUSKEY_COLS)
trg_buskey_list = ["trg."+x for x in SRC_BUSKEY_COLS]
trg_buskey_str = ', '.join(trg_buskey_list)
print(trg_buskey_str)
dbutils.widgets.text("trg_buskey_str",trg_buskey_str)


DeltaTable.createIfNotExists(spark) \
  .tableName(TRG_SCHEMA_NAME + "." + TRG_TBL_NAME) \
  .addColumn("HK_" + root_name + "_ID", "bigint") \
  .addColumns(trg_schema) \
  .addColumn("LOB_ID", "string") \
  .addColumn("MD_REC_SRC", "string") \
  .addColumn("MD_REC_SRC_ID", "bigint") \
  .addColumn("MD_LOAD_DTS", "timestamp") \
  .addColumn("MD_JOB_RUN_ID", "bigint") \
  .addColumn("MD_TASK_RUN_ID", "bigint") \
  .execute()
