# Databricks notebook source
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

# MAGIC %sh
# MAGIC cd /dbfs/FileStore/shared_uploads/bolivarc@fordellconsulting.com/auction_poc
# MAGIC # rm TestDataMappings.xlsx
# MAGIC rm AuctionDataMappings.xlsx
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
