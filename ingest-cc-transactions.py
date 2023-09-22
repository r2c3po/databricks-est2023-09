# Databricks notebook source
# MAGIC %md
# MAGIC #Drop Tables
# MAGIC * Remove from Catalog
# MAGIC * Remove dbfs files

# COMMAND ----------

# %sql
# drop table StgCcDebitTransactions

# COMMAND ----------

# %sh
# cd /dbfs/FileStore/tables/
# rm -r StgCcDebitTransactions

# COMMAND ----------

# %sql
# drop table CcDebitTransactions

# COMMAND ----------

# %sh
# # remove table directory for CcDebitTransactions
# rm -r "/dbfs/FileStore/tables/CcDebitTransactions"

# COMMAND ----------

# MAGIC %md
# MAGIC #Reset Checkpoints
# MAGIC * example only
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC # Reset CheckPoint for files
# MAGIC
# MAGIC rm -r "/dbfs/FileStore/tables/CcDebitTransactions/_checkpoint"
# MAGIC ls -ltr "/dbfs/FileStore/tables/CcDebitTransactions/"
# MAGIC
# MAGIC
# MAGIC # rm -r "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# MAGIC # mkdir "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# MAGIC # ls -ltr "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# MAGIC # echo "CheckPoint Data removed"

# COMMAND ----------

# MAGIC %md
# MAGIC #Create Tables
# MAGIC * StgCcDebitTransactions
# MAGIC * CcDebitTransactions
# MAGIC

# COMMAND ----------

## Create STG table if not already there
from delta.tables import*


deltaTablePath = "dbfs:/FileStore/tables/"
tableName = "StgCcDebitTransactions"


DeltaTable.createIfNotExists(spark) \
  .tableName("default.StgCcDebitTransactions") \
  .addColumn("FileName", "STRING") \
  .addColumn("FileTimestamp", "TIMESTAMP") \
  .addColumn("TransactionDt", "DATE") \
  .addColumn("TransactionTimestamp", "TIMESTAMP") \
  .addColumn("TransactionDescr", "STRING") \
  .addColumn("DebitAmt", "FLOAT") \
  .location(deltaTablePath + tableName) \
  .execute()



# COMMAND ----------

# Create Silver table
# for simplicity we assume no transactions on the same day for the same place for the same amount (only good for PoC)
# Since we only append into STG, there might be duplicates.  Remove them based on TransactionDt, TransactionDescr and DebitAmt
## Create table if not already there
from delta.tables import*

deltaTablePath = "dbfs:/FileStore/tables/"
tableName = "CcDebitTransactions"

DeltaTable.createIfNotExists(spark) \
  .tableName("default.CcDebitTransactions") \
  .addColumn("TransactionDt", "DATE") \
  .addColumn("TransactionDescr", "STRING") \
  .addColumn("DebitAmt", "FLOAT") \
  .addColumn("DupCount", "LONG") \
  .location(deltaTablePath + tableName) \
  .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC #Start Streams

# COMMAND ----------

# MAGIC %md
# MAGIC ##Staging/Bronze

# COMMAND ----------

# load to staging table
from pyspark.sql.types import*
from pyspark.sql.functions import*

# issue with spark so parsing date as string first
accountActivitySchema = StructType([    StructField("TransactionDt", StringType(), False),
                                        StructField("TransactionDescr", StringType(), False),
                                        StructField("DebitAmt", FloatType(), True),
                                        StructField("CreditAmt", FloatType(), True),
                                        StructField("BalanceAmt", FloatType(), False)])


accountActivityFileFolder = "dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com"
accountActivityCheckPointPath = "dbfs:/FileStore/tables/StgCcDebitTransactions/_checkpoint"
   
accountActivityFileStream = spark.readStream.load(accountActivityFileFolder, \
    format = "csv", \
    schema = accountActivitySchema, \
    header = False) \
        .withColumn("TransactionDt", to_date(col("TransactionDt"), "MM/dd/yyyy")) \
        .select( \
            col("_metadata.file_name").alias("FileName"), \
            col("_metadata.file_modification_time").alias("FileTimestamp"), \
            "TransactionDt", \
            to_timestamp(col("TransactionDt"),"MM/dd/yyyy").alias("TransactionTimestamp"), \
            "TransactionDescr", \
            "DebitAmt").filter("DebitAmt is not null")

accountActivityDeltaStream = accountActivityFileStream.writeStream.queryName("write_StgCcDebitTransactions").format("delta").option("checkpointLocation", accountActivityCheckPointPath).toTable("StgCcDebitTransactions")



# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver

# COMMAND ----------

# Load from STG to "silver" table as a stream
# Start Stream

# Keep the UDF with this command for now, bc if it isnt created the code still runs, but it gives unexpected behaviour
def mergeToCcDebitTransactions(microDf, BatchId):
    # Remember the micro batch and BatchId get automatically passed we just need to name them
    (CcDebitTransactions.alias("t")
     .merge(
         microDf.alias("s"),
         "s.TransactionDt = t.TransactionDt and s.TransactionDescr = t.TransactionDescr and s.DebitAmt = t.DebitAmt"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute
     )
    
ccDebitTransactionsStreamIn = spark.readStream.format("delta") \
    .option("withEventTimeOrder", "true") \
    .table("stgccdebittransactions") \
    .withWatermark("TransactionTimestamp", "5 days") \
    .groupBy("TransactionDt", "TransactionDescr", "DebitAmt", "TransactionTimestamp") \
    .agg(count("*").alias("DupCount")) 
    

#HOW DOES IT KNOW THE COLUMN TO USE FOR EventTimeOrder???

# we want to have the most up to date daily transactions, but we know that there are delays in transactions being posted
# assuming we can have revisions (delays in posting transactions up to 5 days)
ccDebitTransactionsDeltaStream = ccDebitTransactionsStreamIn \
    .select("TransactionDt", "TransactionDescr", "DebitAmt", "DupCount") \
    .writeStream.format("delta") \
    .queryName("write_CcDebitTransactions") \
    .outputMode("append") \
    .foreachBatch(mergeToCcDebitTransactions) \
    .option("checkpointLocation", "dbfs:/FileStore/tables/CcDebitTransactions/_checkpoint/") \
    .toTable("CcDebitTransactions") 



# COMMAND ----------

# MAGIC %md
# MAGIC #Testing and Exploring

# COMMAND ----------

# # TESTING
# from pyspark.sql.types import*
# from pyspark.sql.functions import*
# import os

# #@fn.udf(StringType())
# #@udf(returnType=StringType()) 
# #def getFilename(fullPath):
# #    #return fullPath.split("/")[-1]
# #    return os.path.basename(fullPath)

# accountActivitySchema = StructType([StructField("TransactionDt", DateType(), False),
#                                     StructField("TransactionDescr", StringType(), False),
#                                     StructField("DebitAmt", FloatType(), True),
#                                     StructField("CreditAmt", FloatType(), True),
#                                     StructField("BalanceAmt", FloatType(), False)])

# df = spark.read.load("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18_2.csv", format = "csv", schema = accountActivitySchema, header = False, dateFormat = "MM/dd/yyyy").select(col("_metadata.file_name").alias("FileName"),col("_metadata.file_modification_time").alias("FileTimestamp"),"*")
# #.withColumn("FileName", getFilename(input_file_name()) )

# display(df.limit(5))

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /
# MAGIC cd /dbfs
# MAGIC cd FileStore
# MAGIC cd tables
# MAGIC cd StgCcDebitTransactions
# MAGIC
# MAGIC
# MAGIC ls
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC ## TESTING copying files
# MAGIC cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com
# MAGIC ls -l /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com
# MAGIC #cp /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18.csv /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18_2.csv
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC cd "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com"
# MAGIC ls
# MAGIC cat accountactivity_eod_2023_09_17_download_2023_09_18_2.csv
# MAGIC

# COMMAND ----------

# troubleshoot losing dates
# load to staging table
from pyspark.sql.types import*
from pyspark.sql.functions import*

# issue with spark so parsing date as string first
accountActivitySchema = StructType([    StructField("TransactionDt", StringType(), False),
                                        StructField("TransactionDescr", StringType(), False),
                                        StructField("DebitAmt", FloatType(), True),
                                        StructField("CreditAmt", FloatType(), True),
                                        StructField("BalanceAmt", FloatType(), False)])

accountActivityFileFolder = "dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com"
accountActivityCheckPointPath = "dbfs:/FileStore/tables/StgCcDebitTransactions/_checkpoint"

df = spark.read.load(accountActivityFileFolder, \
    format = "csv", \
    schema = accountActivitySchema, \
    header = False) \
        .withColumn("TransactionDt", to_date(col("TransactionDt"), "MM/dd/yyyy")) \
        .select( \
            col("_metadata.file_name").alias("FileName"), \
            col("_metadata.file_modification_time").alias("FileTimestamp"), \
            "TransactionDt", \
            to_timestamp(col("TransactionDt"),"MM/dd/yyyy").alias("TransactionTimestamp"), \
            "TransactionDescr", \
            "DebitAmt").filter("DebitAmt is not null")

# df = spark.createDataFrame([(1,2,3)])
display(df)




# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*),transactiondt 
# MAGIC --  from StgCcDebitTransactions 
# MAGIC --  --where transactiondt is not null 
# MAGIC -- group by transactiondt 
# MAGIC -- order by transactiondt
# MAGIC
# MAGIC select *
# MAGIC  from StgCcDebitTransactions  --where DebitAmt is null
# MAGIC order by 3,4,5 
# MAGIC
# MAGIC -- select  date_format(TransactionDt, 'MM/dd/yyyy') as TransactionDt,TransactionDescr,DebitAmt
# MAGIC --  from StgCcDebitTransactions  --where DebitAmt is null
# MAGIC -- order by 1,2,3 
# MAGIC
# MAGIC -- 206 rows after first file
# MAGIC -- 360 ( + 154) rows after second file
# MAGIC -- 510 after 3rd file
# MAGIC -- select date_format(date '2023-09-14', "MM/dd/yyyy");
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read staging delta table
# MAGIC -- select count(*) from StgCcDebitTransactions;
# MAGIC -- select * from StgCcDebitTransactions order by 3 desc;
# MAGIC
# MAGIC select * from CcDebitTransactions order by 1,2,3; 
# MAGIC --164 rows after 1st file
# MAGIC --164 rows after 2nd file (nothing in the past gets loaded)
# MAGIC -- 936 rows after rebuilding table
# MAGIC
# MAGIC -- select * from CcDebitTransactions where DupCount > 1;
# MAGIC -- select * from CcDebitTransactions where DebitAmt is null;
# MAGIC
# MAGIC -- select max( transactiondt) from StgCcDebitTransactions; --2023-09-19
# MAGIC
# MAGIC -- select max( transactiondt) from CcDebitTransactions; --2023-09-14
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs
# MAGIC cd FileStore
# MAGIC cd shared_uploads
# MAGIC cd bolivarc@fordellconsulting.com
# MAGIC
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC #Stop Streams

# COMMAND ----------

# ## STOP stream
# accountActivityDeltaStream.stop()

# COMMAND ----------

# # Stop Stream (because this is a PoC)
# ccDebitTransactionsDeltaStream.stop();
