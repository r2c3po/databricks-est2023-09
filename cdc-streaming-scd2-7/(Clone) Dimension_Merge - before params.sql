-- Databricks notebook source
-- drop table cdc_streaming.cc_category_dim ;
-- create table cdc_streaming.cc_category_dim (
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

WITH 

src_AgeRank as(
select 
   md_id,
   trxn_desc,
   trxn_Category,
  --  ROW_NUMBER() OVER(PARTITION BY trxn_desc ORDER BY md_file_ts DESC) AS AgeRank,
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

 MERGE into cdc_streaming.cc_category_dim as trg
 using
  ( 
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
      --where new.AgeRank = 1 
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
      where --new.AgeRank = 1 and
        (new.hsh <> old.hsh) -- only when there are changes
        

  ) as src
on src.match_key = trg.trxn_desc 

-- expire these
WHEN MATCHED AND src.new_hsh <> src.old_hsh THEN
 update set
    md_curr_ind = false,
    md_end_ts = NVL(new_md_file_ts,current_timestamp) -- TODO replace with a passed parameter harvested from the data.  in this case we have the file timestamp of the new snapshot, maybe workflow metadata? 


WHEN NOT MATCHED by TARGET THEN 
-- insert bc a new key is introduced in the new snapshot or there is a change  
-- if the record is new (old_md_dkey is null) then skey and dkey are the same since is is a start of a new lineage for this key
-- if the record is an update, then dkey is set to previous dkey.  This is done with the NVL
  insert (
    md_skey    , md_dkey                     , trxn_desc    , trxn_Category    , md_file_name    , md_file_ts    , md_curr_ind, md_start_ts   , md_end_ts
    )
  values(
    new_md_skey, NVL(src.md_dkey,new_md_skey), new_trxn_desc, new_trxn_Category, new_md_file_name, new_md_file_ts, true       , new_md_file_ts, null
    )


WHEN NOT MATCHED by SOURCE and trg.md_curr_ind = true THEN -- expire bc the key does not exist in the new snapshot
  update set
    md_curr_ind = false,
    md_end_ts = current_timestamp -- TODO replace with a passed parameter harvested from the data.  in this case we have the file timestamp of the new snapshot, maybe workflow metadata? 



 ;
