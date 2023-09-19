# Databricks notebook source
# %sql
# drop table StgCcDebitTransactions

# COMMAND ----------

## Create table if not already there
from delta.tables import*
DeltaTable.createIfNotExists(spark) \
  .tableName("default.StgCcDebitTransactions") \
  .addColumn("FileName", "STRING") \
  .addColumn("FileTimestamp", "TIMESTAMP") \
  .addColumn("TransactionDt", "DATE") \
  .addColumn("TransactionDescr", "STRING") \
  .addColumn("DebitAmt", "FLOAT") \
  .execute()


# COMMAND ----------

# MAGIC %sh
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

# MAGIC %sh
# MAGIC mkdir "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"

# COMMAND ----------

# load to staging table
from pyspark.sql.types import*
from pyspark.sql.functions import*

accountActivitySchema = StructType([    StructField("TransactionDt", DateType(), False),
                                        StructField("TransactionDescr", StringType(), False),
                                        StructField("DebitAmt", FloatType(), True),
                                        StructField("CreditAmt", FloatType(), True),
                                        StructField("BalanceAmt", FloatType(), False)])

accountActivityStreamSchema = StructType([    StructField("FileName", StringType(), False),
                                        StructField("FileTimestamp", TimestampType(), False),
                                        StructField("TransactionDt", DateType(), False),
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
accountActivityCheckPointPath = "dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/checkpoint"

accountActivityFileStream = spark.readStream.schema(accountActivitySchema).csv(accountActivityFileFolder).select(col("_metadata.file_name").alias("FileName"),col("_metadata.file_modification_time").alias("FileTimestamp"),"TransactionDt","TransactionDescr","DebitAmt")

accountActivityDeltaStream = accountActivityFileStream.writeStream.format("delta").option("checkpointLocation", accountActivityCheckPointPath).toTable("StgCcDebitTransactions")

TODO - format the date !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from StgCcDebitTransactions order by transactiondt

# COMMAND ----------

accountActivityDeltaStream.stop()

# COMMAND ----------

# copy to silver table, removing duplicates
# for simplicity we assume no transactions on the same day for the same place for the same amount (only good for PoC)
# Since we only append into STG, there might be duplicates.  Remove them based on TransactionDt, TransactionDescr and DebitAmt
## Create table if not already there
from delta.tables import*
DeltaTable.createIfNotExists(spark) \
  .tableName("default.CcDebitTransactions") \
  .addColumn("TransactionDt", "DATE") \
  .addColumn("TransactionDescr", "STRING") \
  .addColumn("DebitAmt", "FLOAT") \
  .execute()

# COMMAND ----------

# Load from STG to "silver" table as a stream
# Start Stream
ccDebitTransactionsStreamIn = spark.readStream.format("delta").load("dbfs:/user/hive/warehouse/stgccdebittransactions")

ccDebitTransactionsDeltaStream = ccDebitTransactionsStreamIn.writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/delta/_checkpoints/").toTable("CcDebitTransactions")


# COMMAND ----------

# Stop Stream (because this is a PoC)
#ccDebitTransactionsDeltaStream.stop();

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read staging delta table
# MAGIC --select count(*) from StgCcDebitTransactions;
# MAGIC --select * from StgCcDebitTransactions;
# MAGIC select * from CcDebitTransactions order by 1;
