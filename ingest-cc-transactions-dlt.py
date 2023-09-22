# Databricks notebook source
import dlt
from pyspark.sql.functions import *

accountActivityFileFolder = "dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com"

accountActivitySchema = StructType([    StructField("TransactionDt", StringType(), False),
                                        StructField("TransactionDescr", StringType(), False),
                                        StructField("DebitAmt", FloatType(), True),
                                        StructField("CreditAmt", FloatType(), True),
                                        StructField("BalanceAmt", FloatType(), False)])

@dlt.table(
    name="dlt_stg_cc_transactions"
    comment="raw data plus file metadata"  
)

def dlt_stg_cc_transactions():
  return (
    spark.readStream.load(accountActivityFileFolder, \
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
  )


