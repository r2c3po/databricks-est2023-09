-- Databricks notebook source
-- create table if not exists cdc_streaming.cc_category_dim (
--   md_skey bigint,
--   md_dkey bigint,
--   trxn_desc string,
--   trxn_Category string,
--   md_file_name string,
--   md_file_ts string,
--   md_start_ts timestamp,
--   md_end_ts timestamp,
--   md_curr_ind int
-- )
-- ;

-- COMMAND ----------

-- %python
-- dbutils.widgets.removeAll()
-- dbutils.widgets.text("source_cols","trxn_desc,trxn_Category")
-- dbutils.widgets.text("buskey_cols","trxn_desc")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("new_buskey_str","")
-- MAGIC dbutils.widgets.text("new_inscol_str","")
-- MAGIC dbutils.widgets.text("new_sel_srccol_str","")
-- MAGIC dbutils.widgets.text("old_buskey_str","")
-- MAGIC dbutils.widgets.text("old_sel_srccol_str","")
-- MAGIC dbutils.widgets.text("trg_buskey_str","")
-- MAGIC
-- MAGIC dbutils.widgets.remove("new_buskey_str")
-- MAGIC dbutils.widgets.remove("new_inscol_str")
-- MAGIC dbutils.widgets.remove("new_sel_srccol_str")
-- MAGIC dbutils.widgets.remove("old_buskey_str")
-- MAGIC dbutils.widgets.remove("old_sel_srccol_str")
-- MAGIC dbutils.widgets.remove("trg_buskey_str")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import ipywidgets as widgets
-- MAGIC from ipywidgets import interact
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # build bus key list
-- MAGIC buskey_str = dbutils.widgets.get("buskey_cols")
-- MAGIC print(buskey_str)
-- MAGIC buskey_list = [x.strip() for x in buskey_str.split(",")]
-- MAGIC print(buskey_list)
-- MAGIC
-- MAGIC # new bus key
-- MAGIC new_buskey_list = ["new."+x for x in buskey_list]
-- MAGIC print(new_buskey_list)
-- MAGIC new_buskey_str = ', '.join(new_buskey_list)
-- MAGIC print(new_buskey_str)
-- MAGIC dbutils.widgets.text("new_buskey_str",new_buskey_str)
-- MAGIC
-- MAGIC # old bus key
-- MAGIC old_buskey_list = ["old."+x for x in buskey_list]
-- MAGIC print(old_buskey_list)
-- MAGIC old_buskey_str = ', '.join(old_buskey_list)
-- MAGIC print(old_buskey_str)
-- MAGIC dbutils.widgets.text("old_buskey_str",old_buskey_str)
-- MAGIC
-- MAGIC # trg bus key
-- MAGIC trg_buskey_list = ["trg."+x for x in buskey_list]
-- MAGIC trg_buskey_str = ', '.join(trg_buskey_list)
-- MAGIC print(trg_buskey_str)
-- MAGIC dbutils.widgets.text("trg_buskey_str",trg_buskey_str)
-- MAGIC
-- MAGIC # build source col list
-- MAGIC srccol_str = dbutils.widgets.get("source_cols")
-- MAGIC srccol_list = [x.strip() for x in srccol_str.split(",")]
-- MAGIC print(srccol_list)
-- MAGIC
-- MAGIC # build new insert column statement snippet
-- MAGIC new_inscol_list = ["new_" + x  for x in srccol_list]
-- MAGIC new_inscol_str = ', '.join(new_inscol_list)
-- MAGIC print(new_inscol_str)
-- MAGIC dbutils.widgets.text("new_inscol_str",new_inscol_str)
-- MAGIC
-- MAGIC # build new select statment
-- MAGIC new_sel_srccol_list = ["new." + x + " as new_" + x for x in srccol_list]
-- MAGIC new_sel_srccol_str = ', '.join(new_sel_srccol_list)
-- MAGIC print(new_sel_srccol_str)
-- MAGIC dbutils.widgets.text("new_sel_srccol_str",new_sel_srccol_str)
-- MAGIC
-- MAGIC
-- MAGIC # build old select statment
-- MAGIC old_sel_srccol_list = ["old." + x + " as old_" + x for x in srccol_list]
-- MAGIC old_sel_srccol_str = ', '.join(old_sel_srccol_list)
-- MAGIC print(old_sel_srccol_str)
-- MAGIC dbutils.widgets.text("old_sel_srccol_str",old_sel_srccol_str)
-- MAGIC

-- COMMAND ----------

WITH 

-- get most current snapshot of source data for full compare
src_AgeRank as(
  select 
        md_id,
        ${source_cols},
        -- trxn_desc,
        -- trxn_Category,
        --  ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
        md_file_name,
        md_file_ts,
        xxhash64(${source_cols}) hsh
  from  
        cdc_streaming.cc_category
  where 
        md_file_ts = (select  max(md_file_ts) from cdc_streaming.cc_category) -- latest file
 ),

-- get most recent snapshot of target data for full compare
trg_DIM as (
  select
        md_skey,
        md_dkey,
        ${source_cols}, -- trxn_desc,trxn_Category, 
        md_file_name,
        md_file_ts,
        md_curr_ind,
        xxhash64(${source_cols}) hsh
  from  
        cdc_streaming.cc_category_dim
  where 
        md_curr_ind = 1 
 )

-- merge from a query result joining both snapshots (newest snapshot and most recent from target data), 
-- so we can have metadata columns from most recent target records to use in our logic
MERGE into cdc_streaming.cc_category_dim as trg
 using
  ( 
      select 
        xxhash64(${new_buskey_str}) as match_key,
        -- new.trxn_desc as match_key,
        new.md_id as new_md_skey,
        ${new_sel_srccol_str},
        -- new.trxn_desc as new_trxn_desc,
        -- new.trxn_Category as new_trxn_Category, 
        new.md_file_name as new_md_file_name,
        new.md_file_ts as new_md_file_ts,
        new.hsh new_hsh,
        old.md_skey,
        old.md_dkey,
        ${old_sel_srccol_str},
        -- old.trxn_desc as old_trxn_desc,
        -- old.trxn_Category as old_trxn_Category, 
        old.md_file_name as old_md_file_name,
        old.md_file_ts as old_md_file_ts,
        old.hsh old_hsh
      from 
        src_AgeRank new
        left outer join trg_DIM as old on (xxhash64(${new_buskey_str}) = xxhash64(${old_buskey_str}) and  old.md_curr_ind = true)
      --where new.AgeRank = 1 
      --and (new.hsh <> old.hsh or new.hsh is null or old.hsh is null) -- only when there are changes

      UNION ALL
      -- these records will be used to create new versions of the matched records as the new_trxn_desc 
      -- will not match and fall into the WHEN NOT MATCHED by TARGET clause
      select 
        null as match_key,
        new.md_id as new_md_skey,
        ${new_sel_srccol_str},
        -- new.trxn_desc as new_trxn_desc,
        -- new.trxn_Category as new_trxn_Category, 
        new.md_file_name as new_md_file_name,
        new.md_file_ts as new_md_file_ts,
        new.hsh as new_hsh,
        old.md_skey as old_md_skey,
        old.md_dkey as old_md_dkey,
        ${old_sel_srccol_str},
        -- old.trxn_desc as old_trxn_desc,
        -- old.trxn_Category as old_trxn_Category, 
        old.md_file_name as old_md_file_name,
        old.md_file_ts as old_md_file_ts,
        old.hsh as old_hsh
      from 
        src_AgeRank new
        inner join trg_DIM as old on (xxhash64(${new_buskey_str}) = xxhash64(${old_buskey_str}) and  old.md_curr_ind = true)
      where --new.AgeRank = 1 and
        (new.hsh <> old.hsh) -- only when there are changes
        

  ) as src
on src.match_key = xxhash64(${trg_buskey_str}) --trg.trxn_desc 

-- expire these
WHEN MATCHED AND src.new_hsh <> src.old_hsh THEN
 update set
    md_curr_ind = false,
    md_end_ts = NVL(new_md_file_ts,current_timestamp) -- TODO replace with a passed parameter harvested from the data.  in this case we have the file timestamp of the new snapshot, maybe workflow metadata? 


WHEN NOT MATCHED by TARGET THEN 
-- insert bc a new key is introduced in the new snapshot or there is a change and we need a new version of the record
-- if the record is new (old_md_dkey is null) then skey and dkey are the same since is is a start of a new lineage for this business key
-- if the the data for this business key has changed, then dkey is set to previous dkey.  This keeps the lineage of the changes linked by dkey
-- This is done with the NVL
  insert (
    md_skey    , md_dkey                     , ${source_cols}    , md_file_name    , md_file_ts    , md_curr_ind, md_start_ts   , md_end_ts
    )
  values(
    new_md_skey, NVL(src.md_dkey,new_md_skey), ${new_inscol_str}    , new_md_file_name, new_md_file_ts, true       , new_md_file_ts, null
    )


WHEN NOT MATCHED by SOURCE and trg.md_curr_ind = true THEN 
-- expire bc the business key in the old snapshot does not exist in the new snapshot.  This means the lineage has ended.
-- NOTE  TODO this is likely not be desired.  expiring it would make all the fact data disapear from reporting.
-- It's probably better to leave the record active and expire it if there are never any fact records linked to it as a clean up activity
  update set
    md_curr_ind = false,
    md_end_ts = current_timestamp -- TODO replace with a passed parameter harvested from the data.  in this case we have the file timestamp of the new snapshot, maybe workflow metadata? 



 ;
