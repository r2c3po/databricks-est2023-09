# Databricks notebook source
# MAGIC %sql
# MAGIC -- ORG Type 1
# MAGIC -- select * from auction_poc.s_organization
# MAGIC
# MAGIC create or replace view auction_poc.D_ORGANIZATION as
# MAGIC SELECT
# MAGIC 	H.HK_ORGANIZATION_ID as ORGANIZATION_SKEY,  -- get HK as SKEY from HUB
# MAGIC   H.ORGANIZATION_ID,-- get key columns from HUB
# MAGIC   H.LOB_ID,-- get LOB columns from HUB to help understand the scope of the key columns
# MAGIC 	S.NAME, -- list of attributes from SAT
# MAGIC 	S.MD_LOAD_DTS as MD_ORGANIZATION_AS_OF,
# MAGIC 	S.MD_REC_SRC as MD_ORGANIZATION_REC_SRC
# MAGIC From auction_poc.H_ORGANIZATION H
# MAGIC left OUTER JOIN auction_poc.S_ORGANIZATION S   
# MAGIC   On H.HK_ORGANIZATION_ID = S.HK_ORGANIZATION_ID
# MAGIC Where S.MD_LOAD_DTS = (select max(S2.md_load_dts) from auction_poc.S_ORGANIZATION S2
# MAGIC                      where H.HK_ORGANIZATION_ID = S2.HK_ORGANIZATION_ID)
# MAGIC                      ;
# MAGIC
# MAGIC                      
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- for every HUB: 
# MAGIC --   derive the "hk root" and "SKEY select"
# MAGIC --   set the "schema"
# MAGIC --   set the "HUB table name"
# MAGIC --   for every key column:
# MAGIC --     derive the "key col select"
# MAGIC
# MAGIC --   select list of SAT tables where HUB_REF_TABLE = name of HUB
# MAGIC --   for every SAT table (in order of spreadsheet):
# MAGIC --      derive "select SAT attrib"
# MAGIC --      append to "select ALL SAT Attrib"
# MAGIC --      derive "select MD col"
# MAGIC --      append to "select ALL MD col"
# MAGIC --      derive "left outer join clause"
# MAGIC --      append to "all left outer join clauses"
# MAGIC --      derive "where clause"
# MAGIC --      append to "all where clauses"
# MAGIC --
# MAGIC --   derive the select statement
# MAGIC   -- SELECT
# MAGIC   -- {SKEY select}
# MAGIC   -- {key col select}
# MAGIC   -- {select ALL SAT Attrib}
# MAGIC   -- {select ALL MD col}
# MAGIC   -- FROM {schema}.{HUB table name} H
# MAGIC   -- {all left outer join clauses}
# MAGIC   -- {all where clauses}
# MAGIC   -- build a view from select statement
# MAGIC   -- create or replace view D_{hk root}
# MAGIC
# MAGIC SELECT
# MAGIC  H.HK_BID_ID as BID_SKEY, -- get HK as SKEY from HUB derive from hk root
# MAGIC  H.AUCTION_ID, -- get key columns from HUB
# MAGIC  H.ORGANIZATION_ID,
# MAGIC  H.ISIN_CD,
# MAGIC  H.MATURITY_DT,
# MAGIC  H.BID_DTS,
# MAGIC  H.BID_RATE,
# MAGIC  H.LOB_ID,
# MAGIC  S.BID_AMT,  -- Main SAT
# MAGIC  S.FINAL_IND, -- Main SAT
# MAGIC  SBA.BID_ALLOTTED_AMT, -- Second SAT 
# MAGIC  S.MD_LOAD_DTS as MD_BID_AS_OF, -- Main SAT
# MAGIC  S.MD_REC_SRC as MD_BID_REC_SRC, -- Main SAT
# MAGIC  SBA.MD_LOAD_DTS as MD_BID_ALLOT_AS_OF, -- Second SAT
# MAGIC  SBA.MD_REC_SRC as MD_BID_ALLOT_REC_SRC -- Second SAT
# MAGIC FROM auction_poc.H_BID H
# MAGIC
# MAGIC LEFT OUTER JOIN auction_poc.S_BID S -- Main SAT
# MAGIC  USING (HK_BID_ID)
# MAGIC
# MAGIC LEFT OUTER JOIN auction_poc.S_BID_ALLOT SBA -- Second SAT
# MAGIC  USING (HK_BID_ID)
# MAGIC
# MAGIC WHERE S.MD_LOAD_DTS = (select max(S2.md_load_dts) from auction_poc.S_BID S2 -- Main SAT
# MAGIC                      where H.HK_BID_ID = S2.HK_BID_ID) 
# MAGIC
# MAGIC AND SBA.MD_LOAD_DTS = (select max(S2.md_load_dts) from auction_poc.S_BID_ALLOT S2 -- Second SAT
# MAGIC                      where H.HK_BID_ID = S2.HK_BID_ID)                      
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC                 HK_BID_ID as BID_SKEY,
# MAGIC                 H.AUCTION_ID, H.ORGANIZATION_ID, H.ISIN_CD, H.MATURITY_DT, H.BID_DTS, H.BID_RATE
# MAGIC                 FROM auction_poc.H_BID H

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.h_bid H_B 
# MAGIC left outer join auction_poc.s_bid S_B using (HK_BID_ID)
# MAGIC left outer join auction_poc.s_bid_allot S_BA on (H_B.HK_BID_ID = S_BA.HK_BID_ALLOT_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC select A.AUCTION_ID, I.ISIN_CD, I.MATURITY_DT  from auction_poc.l_instrument_auction L
# MAGIC inner join auction_poc.s_instrument I on (L.HK_INSTRUMENT_ID = I.HK_INSTRUMENT_ID)
# MAGIC inner join auction_poc.s_auction A on (L.HK_AUCTION_ID = A.HK_S_AUCTION_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.auction_poc.s_auction

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete from auction_poc.S_ORGANIZATION
# MAGIC -- where HK_ORGANIZATION_ID = -773927094516776116
# MAGIC
# MAGIC
# MAGIC update auction_poc.S_ORGANIZATION set hk_compare = 888
# MAGIC where HK_ORGANIZATION_ID = -8223057145070851065

# COMMAND ----------

# MAGIC %sql
# MAGIC with latest_rec_by_key as (
# MAGIC   select distinct first(md_id) OVER (PARTITION BY srccol1, srccol2 ORDER BY md_audit_create_ts) as latest_md_id_by_key 
# MAGIC   from auction_poc.STG_TEST_ORGANIZATION
# MAGIC   ),
# MAGIC joined_recs as  (
# MAGIC   select  
# MAGIC       xxhash64(srccol1, srccol2,'AUCTION') as HK_ORGANIZATION_ID, 
# MAGIC       srccol1, 
# MAGIC       srccol2, 
# MAGIC       srccol3,
# MAGIC       latest_md_id_by_key as MD_REC_SRC_ID,
# MAGIC       xxhash64(srccol3) as hk_compare--,
# MAGIC       -- nvl(trg.hk_compare,0) as  trg_hk_compare
# MAGIC   from auction_poc.STG_TEST_ORGANIZATION as src 
# MAGIC   inner join latest_rec_by_key latest on (src.md_id = latest.latest_md_id_by_key)
# MAGIC   -- left outer join auction_poc.S_ORGANIZATION as trg on (xxhash64(src.srccol1, src.srccol2,'AUCTION') = trg.HK_ORGANIZATION_ID)
# MAGIC )
# MAGIC select 
# MAGIC case when joined_recs.hk_compare != nvl(trg.hk_compare,0) then 0 else joined_recs.HK_ORGANIZATION_ID end as match_HK,
# MAGIC * from joined_recs 
# MAGIC left outer join auction_poc.S_ORGANIZATION as trg on (joined_recs.HK_ORGANIZATION_ID = trg.HK_ORGANIZATION_ID)
# MAGIC where hk_compare != nvl(trg.hk_compare,0)

# COMMAND ----------

# MAGIC %sql
# MAGIC with latest_rec_by_key as 
# MAGIC (select distinct first(md_id) OVER (PARTITION BY srccol1, srccol2 ORDER BY md_audit_create_ts) as latest_md_id_by_key from auction_poc.STG_TEST_ORGANIZATION), 
# MAGIC joined_recs as 
# MAGIC (select xxhash64(srccol1, srccol2,'AUCTION') as HK_ORGANIZATION_ID, srccol1, srccol2, srccol3,latest_md_id_by_key as MD_REC_SRC_ID,
# MAGIC xxhash64(srccol3) as hk_compare, nvl(trg.hk_compare,0) as  trg_hk_compare from auction_poc.STG_TEST_ORGANIZATION as src inner join latest_rec_by_key latest on 
# MAGIC (src.md_id = latest.latest_md_id_by_key)left outer join auction_poc.S_ORGANIZATION as trg on (xxhash64(src.srccol1, src.srccol2,'AUCTION') = trg.HK_ORGANIZATION_ID)) select case when hk_compare != trg_hk_compare then 0 else HK_ORGANIZATION_ID end as match_HK,*  from joined_recs where hk_compare != trg_hk_compare

# COMMAND ----------

# MAGIC %sql
# MAGIC with latest_rec_by_key as (
# MAGIC   select distinct first(md_id) OVER (PARTITION BY srccol1, srccol2 ORDER BY md_audit_create_ts) as latest_md_id_by_key 
# MAGIC   from auction_poc.STG_TEST_ORGANIZATION
# MAGIC   ),
# MAGIC joined_recs as  (
# MAGIC   select  
# MAGIC       xxhash64(srccol1, srccol2,'AUCTION') as HK_ORGANIZATION_ID, 
# MAGIC       srccol1, 
# MAGIC       srccol2, 
# MAGIC       srccol3,
# MAGIC       latest_md_id_by_key as MD_REC_SRC_ID,
# MAGIC       xxhash64(srccol3) as hk_compare,
# MAGIC       nvl(trg.hk_compare,0) as  trg_hk_compare
# MAGIC   from auction_poc.STG_TEST_ORGANIZATION as src 
# MAGIC   inner join latest_rec_by_key latest on (src.md_id = latest.latest_md_id_by_key)
# MAGIC   left outer join auction_poc.S_ORGANIZATION trg on (xxhash64(src.srccol1, src.srccol2,'AUCTION') = trg.HK_ORGANIZATION_ID)
# MAGIC )
# MAGIC select 
# MAGIC case when hk_compare != trg_hk_compare then 0 else HK_ORGANIZATION_ID end as match_HK,
# MAGIC * from joined_recs 
# MAGIC  where hk_compare != trg_hk_compare
# MAGIC
# MAGIC -- if hk_compare != trg_hk_compare, then 0 else HK_ORGANIZATION_ID
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with latest_rec_by_key as (
# MAGIC select distinct first(md_id) OVER (PARTITION BY srccol1, srccol2 ORDER BY md_audit_create_ts) as latest_md_id_by_key 
# MAGIC from auction_poc.STG_TEST_ORGANIZATION
# MAGIC )
# MAGIC select distinct 
# MAGIC     xxhash64(srccol1, srccol2,'AUCTION') as HK_ORGANIZATION_ID, 
# MAGIC     srccol1, 
# MAGIC     srccol2, 
# MAGIC     srccol3,
# MAGIC     xxhash64(srccol2) as hash_compare,
# MAGIC     latest_md_id_by_key as MD_REC_SRC_ID 
# MAGIC from auction_poc.STG_TEST_ORGANIZATION as source 
# MAGIC inner join latest_rec_by_key latest on (source.md_id = latest.latest_md_id_by_key)
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.h_instrument

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.s_organization
# MAGIC order by ABC,MD_LOAD_DTS

# COMMAND ----------

# MAGIC %sql
# MAGIC with latest_rec_by_key as (select distinct first(md_id) OVER (PARTITION BY ParticipantId ORDER BY md_audit_create_ts) as latest_md_id_by_key from auction_poc.STG_AUCTION_ORGANIZATION), joined_recs as (select xxhash64(ParticipantId,'AUCTION') as HK_ORGANIZATION_ID, 
# MAGIC ParticipantId, src.Name,
# MAGIC latest_md_id_by_key as MD_REC_SRC_ID,xxhash64(src.Name) as hk_compare,nvl(trg.hk_compare,0) as  trg_hk_compare from auction_poc.STG_AUCTION_ORGANIZATION as src inner join latest_rec_by_key latest on (src.md_id = latest.latest_md_id_by_key)left outer join auction_poc.S_ORGANIZATION as trg on (xxhash64(src.ParticipantId,'AUCTION') = trg.HK_ORGANIZATION_ID)) select case when hk_compare != trg_hk_compare then 0 else HK_ORGANIZATION_ID end as match_hk,*  from joined_recs where hk_compare != trg_hk_compare

# COMMAND ----------

# %sql
# drop table hive_metastore.auction_poc.s_auction;
# drop table hive_metastore.auction_poc.s_bid;
# drop table hive_metastore.auction_poc.s_bid_allot;
# drop table hive_metastore.auction_poc.s_instrument;
# drop table hive_metastore.auction_poc.s_organization;

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc
# MAGIC # rm TestDataMappings.xlsx
# MAGIC # rm AuctionDataMappings.xlsx
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc/landed_files
# MAGIC # rm AuctionResults.csv
# MAGIC ls

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.h_instrument

# COMMAND ----------

# MAGIC %sql
# MAGIC with source as (select distinct xxhash64(srccol1, srccol2, srccol3,'AUCTION') as HK_ELIGIBILITY_ID, xxhash64(srccol1, srccol2,'AUCTION') as HK_ORGANIZATION_ID, xxhash64(srccol3,'AUCTION') as HK_AUCTION_TYPE_ID ,first(source.md_id) OVER (PARTITION BY srccol1, srccol2, srccol3 ORDER BY md_audit_create_ts) as MD_REC_SRC_ID from auction_poc.STG_TEST_ORGANIZATION as source)select * from source where NOT EXISTS (select * from auction_poc.L_ELIGIBILITY as hub where hub.HK_ELIGIBILITY_ID = source.HK_ELIGIBILITY_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from auction_poc.l_eligibility

# COMMAND ----------

# %sql
# drop table auction_poc.h_auction_type;
# drop table auction_poc.h_instrument;
# drop table auction_poc.h_organization;
# drop table auction_poc.l_eligibility;
# drop table auction_poc.s_instrument;
# drop table auction_poc.s_organization;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from  auction_poc.h_auction_type;
# MAGIC -- select * from  auction_poc.h_instrument;
# MAGIC -- select * from  auction_poc.h_organization;
# MAGIC -- select * from  auction_poc.l_eligibility;
# MAGIC -- select * from  auction_poc.s_instrument;
# MAGIC -- select * from  auction_poc.s_organization;
