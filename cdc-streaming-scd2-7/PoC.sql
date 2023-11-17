-- Databricks notebook source
create or replace STREAMING table poc(
  md_skey bigint generated by default as identity,
  md_dkey bigint generated by default as identity,
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