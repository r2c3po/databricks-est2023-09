# Databricks notebook source
## Create table if not already there
from delta.tables import*
DeltaTable.createIfNotExists(spark) \
  .tableName("default.StgCcDebitTransactions") \
  .addColumn("TransactionDt", "DATE") \
  .addColumn("TransactionDescr", "STRING") \
  .addColumn("DebitAmt", "FLOAT") \
  .execute()


# COMMAND ----------

# load to staging table
from pyspark.sql.types import*
from pyspark.sql.functions import*

accountActivitySchema = StructType([StructField("TransactionDt", DateType(), False),
                                    StructField("TransactionDescr", StringType(), False),
                                    StructField("DebitAmt", FloatType(), True),
                                    StructField("CreditAmt", FloatType(), True),
                                    StructField("BalanceAmt", FloatType(), False)])

accountActivityDataFile = spark.read.load("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18.csv", format = "csv", schema = accountActivitySchema, header = False, dateFormat = "MM/dd/yyyy")

display(accountActivityDataFile.limit(5))

ccDebitTransactionsDataFile = accountActivityDataFile.select("TransactionDt", "TransactionDescr", "DebitAmt")

ccDebitTransactionsDataFile = ccDebitTransactionsDataFile.filter("DebitAmt is not null")

display(ccDebitTransactionsDataFile.limit(5))


