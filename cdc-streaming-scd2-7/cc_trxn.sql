-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Create tables

-- COMMAND ----------

-- drop table cdc_streaming.cc_trxn;

-- COMMAND ----------

-- create schema cdc_streaming;

create or replace STREAMING table cc_trxn(
  trxn_dt date,
  trxn_ts timestamp,
  trxn_dt_str string,
  trxn_desc string,
  debit_amt float,
  md_file_name string NOT NULL,
  md_file_ts timestamp NOT NULL
)
as 
  SELECT 
        to_date(_c0,"MM/dd/yyyy") as trxn_dt,
        to_timestamp(_c0,"MM/dd/yyyy") as trxn_ts,
        _c0 as trxn_dt_str,
        _c1 as trxn_desc,
        CAST(_c2 as float) as debit_amt,
        _metadata.file_name as md_file_name, 
        _metadata.file_modification_time as md_file_ts
  FROM
        cloud_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity*.csv", "csv",
        map("header","false",
            "dateFormat","MM/dd/yyyy")
    )

;

-- select * from cc_trxn;

-- SELECT 
--           CAST(_c0 as date) as trxn_dt,
--           to_date(_c0,"MM/dd/yyyy") as trxn_dt,
--           CAST(_c0 as timestamp) as trxn_ts,
--           _c1 as trxn_desc,
--           CAST(_c2 as float) as debit_amt,
--           _metadata.file_name, _metadata.file_modification_time
-- FROM 
-- read_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_Apr_28__2023___May_29__2023.csv",header =>false);


-- COMMAND ----------


create or replace STREAMING table cc_trxn_category(
  trxn_desc string,
  trxn_Category string,
  md_file_name string NOT NULL,
  md_file_ts timestamp NOT NULL
)
as 
  SELECT 
        `transactionDescr` as trxn_desc,
        `Category` as trxn_Category,
        _metadata.file_name as md_file_name, 
        _metadata.file_modification_time as md_file_ts
  FROM
        cloud_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles/CategoryMap*.csv", "csv",
        map("header","true")
    )

;


-- COMMAND ----------

create or replace materialized view cc_trxn_summary as 
select trxn_desc, sum(debit_amt ) as total_debit_amt from LIVE.cc_trxn group by all;

-- COMMAND ----------

-- drop table cdc_streaming.cc_trxn;
