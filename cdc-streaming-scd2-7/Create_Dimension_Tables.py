# Databricks notebook source
dbutils.widgets.text("schema_name","cdc_streaming")
dbutils.widgets.text("src_table_name","cc_category")
dbutils.widgets.text("trg_table_name","cc_category_dim")

# COMMAND ----------

from delta.tables import*

SCHEMA_NAME = dbutils.widgets.get("schema_name")
SRC_TBL_NAME = dbutils.widgets.get("src_table_name")
TRG_TBL_NAME = dbutils.widgets.get("trg_table_name")

src_df = spark.read.format("delta") \
    .table(SCHEMA_NAME + "." + SRC_TBL_NAME)

src_cols = src_df.select([column for column in src_df.columns if not(column.startswith("md_"))])

# schema = StructType([StructField('SignalType', StringType()),StructField('StartTime', TimestampType())])

DeltaTable.createIfNotExists(spark) \
  .tableName(SCHEMA_NAME + "." + TRG_TBL_NAME) \
  .addColumn("md_skey", "bigint") \
  .addColumn("md_dkey", "bigint") \
  .addColumns(src_cols.schema) \
  .addColumn("md_file_name", "string") \
  .addColumn("md_file_ts", "string") \
  .addColumn("md_start_ts", "timestamp") \
  .addColumn("md_end_ts", "timestamp") \
  .addColumn("md_curr_ind", "int") \
  .execute()
