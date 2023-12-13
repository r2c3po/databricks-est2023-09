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
# print(mapping)

# Get list of TRG Tables by SRC_LOB
tablelist = (mapping.query('SRC_LOB == "' + SRC_LOB + '"')[["TRG_TABLE"]])["TRG_TABLE"].unique().tolist()


#!!!!!!!
#TODO Need to run these in parallel
#!!!!!!!

# For each table
for trgtable in tablelist:
    print()
    print("Target Table: " + trgtable)
    root_name = trgtable.lstrip("H_")

    # Get Source Table
    srctablelist = (mapping.query('SRC_LOB == "' + SRC_LOB + '"' + ' and TRG_TABLE == "' + trgtable + '"')[["SRC_TABLE"]])["SRC_TABLE"].unique().tolist()
    if len(srctablelist) == 1 :
        srctable = srctablelist[0]
        print("Source Table: " + srctable)

        # Get Mapping
        trg2srcdf = mapping.sort_index().query('SRC_LOB == "' + SRC_LOB + '"' + ' and TRG_TABLE == "' + trgtable + '"')[["TRG_COL","SRC_COL"]]
        print(trgtable + " Mappings:")
        print(trg2srcdf)

        # Target Columns
        trgcolslist = trg2srcdf["TRG_COL"].values.tolist()
        print(trgcolslist)

        # Source Columns
        srccolslist = trg2srcdf["SRC_COL"].values.tolist()
        print(srccolslist)

        # Get list of bus keys in proper order
        srckeysdf = mapping.sort_values("BUSKEY_ORDER").query('SRC_LOB == "' + SRC_LOB + '"' + ' and TRG_TABLE == "' + trgtable + '" and IS_BUSKEY == True')["SRC_COL"]
        srckeyslist = keysdf.values.tolist()
        print(trgtable + " SrcKeylist:")
        print(srckeyslist)
        srckeycols = ', '.join(srckeyslist)
  

        # HK column name
        hkcol = "HK_" + root_name + "_ID"

        # HK select clause
        hkselect = "xxhash64(" + srckeycols + ") as " + hkcol
        print("HK select clause: " + hkselect)
        #xxhash64(trxn_desc, trxn_Category) hsh
        
        # trgcols = ["old."+x for x in buskey_list]

        # insertstmt = "insert into " + trgtable + "(" + hkcol + "," + ', '.join(trgcolslist) + ") " + \
        #              "select " + hkselect + ", " + ', '.join(srccolslist) + " from " + srctable + " as source where NOT EXISTS " +\
        #              "(select * from " + trgtable + " as hub where hub."+ hkcol +" = source." + hkcol + ")"

        # print("Insert Statement: " + insertstmt)

        selectstmt = "select " + hkselect + ", " + ', '.join(srccolslist) + " from " + srctable + " as source where NOT EXISTS " +\
                     "(select * from " + trgtable + " as hub where hub."+ hkcol +" = source." + hkcol + ")"
        
        print("Select Statementr: " + selectstmt)

        srcdf = spark.sql(selectstmt)

        #TODO  Need to use this I think https://docs.databricks.com/en/delta/merge.html#language-python

        

     


# COMMAND ----------

# MAGIC %md
# MAGIC #Adhoc cleanup

# COMMAND ----------

# %sql
# drop table auction_poc.H_ORGANIZATION;
# drop table auction_poc.H_INSTRUMENT;
# drop table auction_poc.TEST;
