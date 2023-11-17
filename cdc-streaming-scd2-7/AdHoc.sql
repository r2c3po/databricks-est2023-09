-- Databricks notebook source
-- select * from cdc_streaming.cc_trxn_summary;
with AgeRank as (
  select 
    ROW_NUMBER() OVER(PARTITION BY trxn_dt, trxn_desc, debit_amt ORDER BY md_file_ts DESC) AS AgeRank
    ,*
  from 
    cdc_streaming.cc_trxn
)
select 
  trxn_dt, trxn_ts, trxn_dt_str, trxn_desc, debit_amt, md_file_name, md_file_ts   
from 
  AgeRank 
where 
  AgeRank.AgeRank = 1
;
  

-- COMMAND ----------

select * from cdc_streaming.poc;

-- COMMAND ----------

SELECT 
          to_date(_c0,"MM/dd/yyyy") as trxn_dt,
          CAST(_c0 as timestamp) as trxn_ts,
          _c1 as trxn_desc,
          CAST(_c2 as float) as debit_amt,
          _metadata.file_name as md_file_name, 
          _metadata.file_modification_time as md_file_ts,
           xxhash64(curr.trxn_desc, curr.trxn_Category) curr.hsh
      --     xxhash64(_c0, _c1, _c2) value
    FROM
          read_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_Apr_28__2023___May_29__2023.csv",header =>false);

;

-- COMMAND ----------

SELECT 
*
    FROM
          read_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/accountactivity_Apr_28__2023___May_29__2023.csv",header =>false);

;

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC
-- MAGIC cd "/dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com"
-- MAGIC cd StaticFiles
-- MAGIC ls -l
-- MAGIC cat CategoryMap.csv

-- COMMAND ----------

  SELECT 
        transactionDescr as trxn_desc,
        Category as trxn_Category,
        _metadata.file_name as md_file_name, 
        _metadata.file_modification_time as md_file_ts
  FROM
        read_files("dbfs:/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles/CategoryMap.csv")

-- COMMAND ----------

select 
   trxn_desc,
   trxn_Category
 from cdc_streaming.cc_category;

 

-- COMMAND ----------

with AgeRank as(
select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts
 from cdc_streaming.cc_category
 )
 select * from AgeRank where AgeRank = 1
 and 
 ;

-- COMMAND ----------

select * from cdc_streaming.cc_category_dim 

-- COMMAND ----------

with src_AgeRank as(
select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts
 from cdc_streaming.cc_category
 )
   select 
    curr.md_id as curr_md_skey,
    curr.trxn_desc as curr_trxn_desc,
    curr.trxn_Category as curr_trxn_Category, 
    curr.md_file_name as curr_md_file_name,
    curr.md_file_ts as curr_md_file_ts,
    trg.md_skey,
    trg.md_dkey,
    trg.trxn_desc as trg_trxn_desc,
    trg.trxn_Category as trg_trxn_Category, 
    trg.md_file_name as trg_md_file_name,
    trg.md_file_ts as trg_md_file_ts
   from 
   src_AgeRank curr
   full outer join cdc_streaming.cc_category_dim trg on (curr.trxn_desc = trg.trxn_desc and md_curr_ind = 1 )
   where curr.AgeRank = 1 
   and curr.value <> trg.value
   ;

  --  md_start_ts timestamp,
  -- md_end_ts timestamp,
  -- md_curr_ind int

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/StaticFiles
-- MAGIC # rm CategoryMap_2-1.csv
-- MAGIC #shared_uploads/bolivarc@fordellconsulting.com/
-- MAGIC # mv CategoryMap_2.csv StaticFiles
-- MAGIC # rm CategoryMap_5_new_cat_NEW_chng_RING_delete_VIA.csv
-- MAGIC ls -l
-- MAGIC

-- COMMAND ----------

select * from cdc_streaming.cc_category where trxn_desc = 'RING MONTHLY PLAN' or  trxn_desc = 'NEW' or trxn_desc like '%VIA%' order by trxn_desc,md_file_ts;

-- COMMAND ----------

select * from cdc_streaming.cc_category_dim where trxn_desc = 'RING MONTHLY PLAN' or  trxn_desc = 'NEW' or trxn_desc like '%VIA%' 
order by trxn_desc,md_file_ts;

-- COMMAND ----------

with a as 
(
select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts,
   xxhash64(trxn_desc, trxn_Category) hsh
 from cdc_streaming.cc_category
 where md_file_ts = (select  max(md_file_ts) from cdc_streaming.cc_category)
)
select * from a
 where (trxn_desc = 'RING MONTHLY PLAN' or  trxn_desc = 'NEW' or trxn_desc like '%VIA%' )
-- and AgeRank = 1


-- select max (md_file_ts) from cdc_streaming.cc_category

-- COMMAND ----------

select * from cdc_streaming.cc_category_dim where trxn_desc = 'RING MONTHLY PLAN' order by md_file_ts;

-- COMMAND ----------

with a as (select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts,
   xxhash64(trxn_desc, trxn_Category) hsh
 from cdc_streaming.cc_category
 where md_file_ts = (select  max(md_file_ts) from cdc_streaming.cc_category) -- latest file
)
select count(*) , trxn_desc from a
group by trxn_desc having count(*) > 1

-- COMMAND ----------

WITH 

src_AgeRank as(
select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts,
   xxhash64(trxn_desc, trxn_Category) hsh
 from cdc_streaming.cc_category
 where md_file_ts = (select  max(md_file_ts) from cdc_streaming.cc_category) -- latest file
 ),

 trg_DIM as (
  select
        md_skey,
        md_dkey,
        trxn_desc,
        trxn_Category, 
        md_file_name,
        md_file_ts,
        md_curr_ind,
        xxhash64(trxn_desc, trxn_Category) hsh
  from cdc_streaming.cc_category_dim
  where md_curr_ind = 1 
 )
,
a as (

      select 
        new.trxn_desc as match_key,
        new.md_id as new_md_skey,
        new.trxn_desc as new_trxn_desc,
        new.trxn_Category as new_trxn_Category, 
        new.md_file_name as new_md_file_name,
        new.md_file_ts as new_md_file_ts,
        new.hsh new_hsh,
        old.md_skey,
        old.md_dkey,
        old.trxn_desc as old_trxn_desc,
        old.trxn_Category as old_trxn_Category, 
        old.md_file_name as old_md_file_name,
        old.md_file_ts as old_md_file_ts,
        old.hsh old_hsh
      from 
        src_AgeRank new
        left outer join trg_DIM as old on (new.trxn_desc = old.trxn_desc and  old.md_curr_ind = true)
      where new.AgeRank = 1 
      --and (new.hsh <> old.hsh or new.hsh is null or old.hsh is null) -- only when there are changes

      UNION ALL
      -- these records will be used to create new versions of the matched records as the new_trxn_desc 
      -- will not match and fall into the WHEN NOT MATCHED by TARGET clause
      select 
        null as match_key,
        new.md_id as new_md_skey,
        new.trxn_desc as new_trxn_desc,
        new.trxn_Category as new_trxn_Category, 
        new.md_file_name as new_md_file_name,
        new.md_file_ts as new_md_file_ts,
        new.hsh as new_hsh,
        old.md_skey as old_md_skey,
        old.md_dkey as old_md_dkey,
        old.trxn_desc as old_trxn_desc,
        old.trxn_Category as old_trxn_Category, 
        old.md_file_name as old_md_file_name,
        old.md_file_ts as old_md_file_ts,
        old.hsh as old_hsh
      from 
        src_AgeRank new
        inner join trg_DIM as old on (new.trxn_desc = old.trxn_desc and  old.md_curr_ind = true)
      where new.AgeRank = 1 and
        (new.hsh <> old.hsh) -- only when there are changes
        
)
select count(*) , match_key from a
group by match_key having count(*) > 1

  

-- COMMAND ----------

with

src_AgeRank as(
select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts,
   xxhash64(trxn_desc, trxn_Category) hsh
 from cdc_streaming.cc_category
 ),

 trg_DIM as (
  select
        md_skey,
        md_dkey,
        trxn_desc,
        trxn_Category, 
        md_file_name,
        md_file_ts,
        xxhash64(trxn_desc, trxn_Category) hsh
  from cdc_streaming.cc_category_dim
  where md_curr_ind = 1 
 )


      select 
        curr.md_id as curr_md_skey,
        curr.trxn_desc as curr_trxn_desc,
        curr.trxn_Category as curr_trxn_Category, 
        curr.md_file_name as curr_md_file_name,
        curr.md_file_ts as curr_md_file_ts,
        curr.hsh curr_hsh,
        trg_DIM.md_skey,
        trg_DIM.md_dkey,
        trg_DIM.trxn_desc as new_trxn_desc,
        trg_DIM.trxn_Category as new_trxn_Category, 
        trg_DIM.md_file_name as new_md_file_name,
        trg_DIM.md_file_ts as new_md_file_ts,
        trg_DIM.hsh new_hsh
      from 
        src_AgeRank curr
        full outer join trg_DIM on (curr.trxn_desc = trg_DIM.trxn_desc)
      where curr.AgeRank = 1 
      and (curr.hsh <> trg_DIM.hsh or curr.hsh is null or trg_DIM.hsh is null)


-- COMMAND ----------

WITH 

src_AgeRank as(
select 
   md_id,
   trxn_desc,
   trxn_Category,
   ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
   md_file_name,
   md_file_ts,
   xxhash64(trxn_desc, trxn_Category) hsh
 from cdc_streaming.cc_category
 ),

 trg_DIM as (
  select
        md_skey,
        md_dkey,
        trxn_desc,
        trxn_Category, 
        md_file_name,
        md_file_ts,
        xxhash64(trxn_desc, trxn_Category) hsh
  from cdc_streaming.cc_category_dim
  where md_curr_ind = 1 
 )

--  MERGE into cdc_streaming.cc_category_dim as trg
--  using
--   ( 
      select 
        new.md_id as new_md_skey,
        new.trxn_desc as new_trxn_desc,
        new.trxn_Category as new_trxn_Category, 
        new.md_file_name as new_md_file_name,
        new.md_file_ts as new_md_file_ts,
        new.hsh new_hsh,
        old.md_skey,
        old.md_dkey,
        old.trxn_desc as old_trxn_desc,
        old.trxn_Category as old_trxn_Category, 
        old.md_file_name as old_md_file_name,
        old.md_file_ts as old_md_file_ts,
        old.hsh old_hsh
      from 
        src_AgeRank new
        full outer join trg_DIM as old on (new.trxn_desc = old.trxn_desc)
      where new.AgeRank = 1 
      and (new.hsh <> old.hsh or new.hsh is null or old.hsh is null)
--   ) as src
-- on src.old_trxn_desc = trg.trxn_Category

-- --WHEN NOT MATCHED AND (new_hsh is null) THEN -- expire bc the key does not exist in the new snapshot

-- WHEN NOT MATCHED AND (old_hsh is null) THEN -- insert bc a new key is introduced in the new snapshot.  skey and dkey are the same since is is a start of a new lineage for this key
--   insert (md_skey, md_dkey, trxn_desc, trxn_Category, md_file_name, md_file_ts)
--   values(
--     new_md_skey, new_md_skey, new_trxn_desc, new_trxn_Category, new_md_file_name, new_md_file_ts
--   )

-- -- WHEN MATCHED AND () THEN
-- --   insert (md_skey, md_dkey, trxn_desc, trxn_Category, md_file_name, md_file_ts)
-- --   values(
-- --     src.md_id, src.md_id, trxn_desc, trxn_Category, md_file_name, md_file_ts
-- --   )  
--  ;

-- COMMAND ----------

select * from cdc_streaming.cc_category

-- COMMAND ----------

select * from cdc_streaming.cc_category_dim
