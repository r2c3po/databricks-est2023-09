-- Databricks notebook source
-- CC_CATEGORY
create or replace STREAMING table cc_category(
  -- md_id bigint generated by default as identity,
  -- trxn_desc string,
  -- trxn_Category string,
  -- md_file_name string, 
  -- -- md_file_ts timestamp
)
;
copy into cc_category from
(
  SELECT 
        uuid() as md_id,
        *
        -- transactionDescr as trxn_desc,
        -- Category as trxn_Category,
        _metadata.file_name as md_file_name, 
        _metadata.file_modification_time as md_file_ts,
        current_timestamp as md_load_ts
  FROM
        cloud_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles/CategoryMap*.csv", "csv",
        map("header","true")
    )
)
;


  -- SELECT 
  --       transactionDescr as trxn_desc,
  --       Category as trxn_Category,
  --       _metadata.file_name as md_file_name, 
  --       _metadata.file_modification_time as md_file_ts
  -- FROM
  --       read_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles/CategoryMap.csv")