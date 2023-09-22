# Databricks notebook source
# %sql
# drop table StgCcDebitTransactions

# COMMAND ----------

# %sh
# cd /dbfs/FileStore/tables/
# rm -r StgCcDebitTransactions

# COMMAND ----------

## Create table if not already there
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

# %sh
# # Reset CheckPoint for files
# #ls -ltr "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# rm -r "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# mkdir "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# ls -ltr "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"
# echo "CheckPoint Data removed"

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

# load to staging table
from pyspark.sql.types import*
from pyspark.sql.functions import*

# issue with spark so parsing date as string first
accountActivitySchema = StructType([    StructField("TransactionDt", StringType(), False),
                                        StructField("TransactionDescr", StringType(), False),
                                        StructField("DebitAmt", FloatType(), True),
                                        StructField("CreditAmt", FloatType(), True),
                                        StructField("BalanceAmt", FloatType(), False)])


#################################################
# This is START of the the old batch code.
    # accountActivityDataFile = spark.read.load("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18_2.csv", format = "csv", schema = accountActivitySchema, header = False, dateFormat = "MM/dd/yyyy").select(col("_metadata.file_name").alias("FileName"),col("_metadata.file_modification_time").alias("FileTimestamp"),"*")

    # display(accountActivityDataFile.limit(5))

    # ccDebitTransactionsDataFile = accountActivityDataFile.select("FileName","FileTimestamp","TransactionDt", "TransactionDescr", "DebitAmt")

    # ccDebitTransactionsDataFile = ccDebitTransactionsDataFile.filter("DebitAmt is not null")

    # display(ccDebitTransactionsDataFile.limit(5))

    # ccDebitTransactionsDataFile.write.format("delta").mode("append").saveAsTable("StgCcDebitTransactions")
# This is END of the the old batch code.
##########################################################

accountActivityFileFolder = "dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com"
accountActivityCheckPointPath = "dbfs:/FileStore/tables/StgCcDebitTransactions/_checkpoint"


#b4 using read.load syntax
# accountActivityFileStream = spark.readStream.format("csv").schema(accountActivitySchema).option("date_format", "MM/dd/yyyy").load(accountActivityFileFolder). \
#     select( \
#         col("_metadata.file_name").alias("FileName"), \
#         col("_metadata.file_modification_time").alias("FileTimestamp"), \
#         "TransactionDt", \
#         # to_date(col("TransactionDt"),"MM/dd/yyyy").alias("TransactionDt"), \
#         "TransactionDescr", \
#         "DebitAmt")
    
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

# MAGIC %sql
# MAGIC -- select count(*),transactiondt 
# MAGIC --  from StgCcDebitTransactions 
# MAGIC --  --where transactiondt is not null 
# MAGIC -- group by transactiondt 
# MAGIC -- order by transactiondt
# MAGIC
# MAGIC select *
# MAGIC  from StgCcDebitTransactions  --where DebitAmt is null
# MAGIC order by transactiondt desc
# MAGIC
# MAGIC -- select date_format(date '2023-09-14', "MM/dd/yyyy");
# MAGIC
# MAGIC

# COMMAND ----------

# ## STOP stream
# accountActivityDeltaStream.stop()

# COMMAND ----------

# %sql
# drop table CcDebitTransactions

# COMMAND ----------

# %sh
# # remove table directory for CcDebitTransactions
# rm -r "/dbfs/FileStore/tables/CcDebitTransactions"

# COMMAND ----------

# copy to silver table, removing duplicates
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

def mergeToCcDebitTransactions(microDf, atchId):
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


# COMMAND ----------

# Load from STG to "silver" table as a stream
# Start Stream
# ccDebitTransactionsStreamIn = spark.readStream.format("delta").load("dbfs:/user/hive/warehouse/stgccdebittransactions")

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

# # Stop Stream (because this is a PoC)
# ccDebitTransactionsDeltaStream.stop();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read staging delta table
# MAGIC -- select count(*) from StgCcDebitTransactions;
# MAGIC -- select * from StgCcDebitTransactions order by 3 desc;
# MAGIC
# MAGIC select * from CcDebitTransactions order by 1 desc;
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
# MAGIC ls
