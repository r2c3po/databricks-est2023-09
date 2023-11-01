# Databricks notebook source
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
# MAGIC # mkdir StaticFiles
# MAGIC mv CategoryMap.csv ./StaticFiles
# MAGIC ls -l /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com
# MAGIC #cp /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18.csv /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_eod_2023_09_17_download_2023_09_18_2.csv
# MAGIC # cat accountactivity_Feb_28_2023___Mar_27_2023.csv
# MAGIC # rm accountactivity_Feb_28_2023___Mar_27_2023.csv
# MAGIC # rm accountactivity_Apr_27_2023_One_FAKE_001.csv
# MAGIC # rm *.csv

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

# MAGIC %sql
# MAGIC select * from CcDebitTransactions order by 1,2,3; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CcDebitTransactions order by 1,2,3; 

# COMMAND ----------

# MAGIC %md
# MAGIC #Temp Views

# COMMAND ----------

# MAGIC %md
# MAGIC ##CategoryMap

# COMMAND ----------

# MAGIC %sql
# MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC CREATE or REPLACE TEMPORARY VIEW CategoryMap
# MAGIC USING CSV
# MAGIC OPTIONS (path "dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles/CategoryMap.csv", header "true", mode "FAILFAST")
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC ##CcDebitTransactions_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW CcDebitTransactions_clean
# MAGIC AS
# MAGIC select 
# MAGIC  transactionDescr as orig
# MAGIC ,case  
# MAGIC   when left(transactionDescr,13) = 'AMZN Mktp CA*' then 'AMZN Mktp CA'
# MAGIC   when left(transactionDescr,10) = 'Amazon.ca*' then 'Amazon.ca'
# MAGIC   when left(transactionDescr,11) = 'Audible CA*' then 'Audible CA*'
# MAGIC   when left(transactionDescr, 5) = 'A & W' then 'A&W'
# MAGIC   when left(transactionDescr, 3) = 'A&W' then 'A&W'
# MAGIC   when left(transactionDescr, 8) = 'BEST BUY' then 'BEST BUY'
# MAGIC   when left(transactionDescr,13) = 'BOOSTER JUICE' then 'BOOSTER JUICE'
# MAGIC   when left(transactionDescr,14) = 'CDN TIRE STORE' then 'CDN TIRE STORE'
# MAGIC   when left(transactionDescr, 8) = 'CINEPLEX' then 'CINEPLEX'
# MAGIC   when left(transactionDescr, 8) = 'CIRCLE K' then 'CIRCLE K'
# MAGIC   when left(transactionDescr,20) = 'CITY OF OTTAWA - LOT' then 'CITY OF OTTAWA PARKING'
# MAGIC   when left(transactionDescr,10) = 'COUCHETARD' then 'COUCHETARD'
# MAGIC   when left(transactionDescr,11) = 'DAIRY QUEEN' then 'DAIRY QUEEN'
# MAGIC   when left(transactionDescr, 9) = 'DOLLARAMA' then 'DOLLARAMA'
# MAGIC   when left(transactionDescr,11) = 'DOLLAR TREE' then 'DOLLAR TREE'
# MAGIC   when left(transactionDescr, 8) = 'DQ GRILL' then 'DQ GRILL'
# MAGIC   when left(transactionDescr, 8) = 'FARM BOY' then 'FARM BOY'
# MAGIC   when left(transactionDescr, 8) = "HARVEY'S" then "HARVEY'S"
# MAGIC   when left(transactionDescr, 6) = 'IMPARK' then 'IMPARK'
# MAGIC   when left(transactionDescr, 3) = 'KFC' then 'KFC'
# MAGIC   when left(transactionDescr, 4) = 'LCBO' then 'LCBO'
# MAGIC   when left(transactionDescr, 7) = 'LOBLAWS' then 'LOBLAWS'
# MAGIC   when left(transactionDescr, 7) = "LONGO'S" then "LONGO'S"
# MAGIC   when left(transactionDescr, 7) = 'MACEWEN' then 'MACEWEN'
# MAGIC   when left(transactionDescr, 9) = 'MARSHALLS' then 'MARSHALLS'
# MAGIC   when left(transactionDescr,10) = "MCDONALD'S" then "MCDONALD'S"
# MAGIC   when left(transactionDescr, 5) = 'METRO' then 'METRO'
# MAGIC   when left(transactionDescr, 8) = 'MONTANAS' then 'MONTANAS'
# MAGIC   when left(transactionDescr, 4) = 'MSFT' then 'MSFT'
# MAGIC   when left(transactionDescr,12) = 'PANERA BREAD' then 'PANERA BREAD'
# MAGIC   when left(transactionDescr,11) = 'PARK INDIGO' then 'PARK INDIGO'
# MAGIC   when left(transactionDescr, 8) = 'PET VALU' then 'PET VALU'
# MAGIC   when left(transactionDescr,12) = 'PETRO CANADA' then 'PETRO CANADA'
# MAGIC   when left(transactionDescr, 8) = 'PETROCAN' then 'PETRO CANADA'
# MAGIC   when left(transactionDescr, 7) = 'POPEYES' then 'POPEYES'  
# MAGIC   when left(transactionDescr,10) = 'PrimeVideo' then 'PrimeVideo' 
# MAGIC   when left(transactionDescr, 7) = 'QUICKIE' then 'QUICKIE' 
# MAGIC   when left(transactionDescr,10) = 'SECOND CUP' then 'SECOND CUP' 
# MAGIC   when left(transactionDescr, 7) = 'SEPHORA' then 'SEPHORA' 
# MAGIC   when left(transactionDescr, 5) = 'SHELL' then 'SHELL' 
# MAGIC   when left(transactionDescr, 8) = 'SHOPPERS' then 'SHOPPERS' 
# MAGIC   when left(transactionDescr, 7) = 'STAPLES' then 'STAPLES' 
# MAGIC   when left(transactionDescr, 9) = 'STARBUCKS' then 'STARBUCKS' 
# MAGIC   when left(transactionDescr, 6) = 'SUBWAY' then 'SUBWAY' 
# MAGIC   when left(transactionDescr, 6) = 'Subway' then 'Subway' 
# MAGIC   when left(transactionDescr,16) = 'THE CHOPPED LEAF' then 'THE CHOPPED LEAF' 
# MAGIC   when left(transactionDescr,14) = 'THE SENS STORE' then 'THE SENS STORE' 
# MAGIC   when left(transactionDescr,11) = 'TIM HORTONS' then 'TIM HORTONS' 
# MAGIC   when left(transactionDescr, 9) = 'TOYS R US' then 'TOYS R US' 
# MAGIC   when left(transactionDescr, 8) = 'ULTRAMAR' then 'ULTRAMAR' 
# MAGIC   when left(transactionDescr,12) = 'URBAN PLANET' then 'URBAN PLANET' 
# MAGIC   when left(transactionDescr, 8) = 'VIA RAIL' then 'VIA RAIL' 
# MAGIC   when left(transactionDescr,12) = 'W.O. STINSON' then 'W.O. STINSON' 
# MAGIC   when left(transactionDescr, 8) = 'WAL-MART' then 'WAL-MART' 
# MAGIC   when left(transactionDescr, 7) = "WENDY'S" then "WENDY'S" 
# MAGIC   when left(transactionDescr, 7) = 'WINNERS' then 'WINNERS' 
# MAGIC   else transactionDescr
# MAGIC end as transactionDescr
# MAGIC ,case 
# MAGIC   when left(transactionDescr,13) = 'AMZN Mktp CA*' then substring(transactionDescr,14,100)
# MAGIC   when left(transactionDescr,10) = 'Amazon.ca*' then substring(transactionDescr,11,100)
# MAGIC   when left(transactionDescr,11) = 'Audible CA*' then substring(transactionDescr,12,100)
# MAGIC   when left(transactionDescr, 4) = 'MSFT' then substring(transactionDescr,7,100)
# MAGIC end as AdditionalInfo
# MAGIC ,DebitAmt
# MAGIC ,TransactionDt
# MAGIC ,date_format(TransactionDt, 'yyyy-MM') TransactionMonth
# MAGIC
# MAGIC from CcDebitTransactions

# COMMAND ----------

# MAGIC %md
# MAGIC ##MonthlyLabel

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW MonthlyLabel
# MAGIC AS
# MAGIC select TransactionMonth, 
# MAGIC        round(sum(DebitAmt),2) MonthlyDebitAmt,
# MAGIC        concat(TransactionMonth , ' : $', round(sum(DebitAmt),2)) label
# MAGIC   from CcDebitTransactions_clean 
# MAGIC group by TransactionMonth
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #Visualization

# COMMAND ----------

# MAGIC %sql
# MAGIC select TransactionDt, a.TransactionMonth, label, Category, DebitAmt  
# MAGIC from ccdebittransactions_clean a
# MAGIC inner join CategoryMap m on (a.transactionDescr = m.transactionDescr)
# MAGIC inner join MonthlyLabel l on (a.TransactionMonth = l.TransactionMonth)
# MAGIC where a.TransactionMonth >= '2023-03'
# MAGIC order by 1
# MAGIC ; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select label, round(sum(DebitAmt),2)
# MAGIC from ccdebittransactions_clean a
# MAGIC inner join CategoryMap m on (a.transactionDescr = m.transactionDescr)
# MAGIC inner join MonthlyLabel l on (a.TransactionMonth = l.TransactionMonth)
# MAGIC where a.TransactionMonth >= '2023-03'
# MAGIC and Category = 'Entertainment'
# MAGIC group by label
# MAGIC order by 1
# MAGIC ; 

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/CategoryMap.csv")

# COMMAND ----------



select * from CategoryMap;


# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs
# MAGIC cd FileStore
# MAGIC cd shared_uploads
# MAGIC cd bolivarc@fordellconsulting.com
# MAGIC
# MAGIC ls

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CcDebitTransactions_NoDelay order by 4 desc, 1 desc;
