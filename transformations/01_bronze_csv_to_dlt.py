"""
Bronze layer DLT pipeline - CSV ingestion.

This module registers DLT tables for bronze layer ingestion.
Configuration is imported from bronze_ingestion_config.py.
"""

import dlt
from pyspark.sql import functions as F
from bronze_ingestion_config import source_lst


# =====================================================================
# INGESTION LOGIC UTILITY
# =====================================================================

def create_bronze_pipeline(source_file_path, source_system, dlt_schema, dlt_table_name, catalog_comment, primary_key_field):
    """
    Programmatically registers a DLT pipeline target - Consistent with DRY paradigm.
    
    Args:
        source_file_path: Path to source CSV file
        source_system: Source system identifier for audit trail
        dlt_schema: Expected schema for validation
        dlt_table_name: Name of the DLT table to create
        catalog_comment: UC catalog comment for documentation
        primary_key_field: Name of the primary key field (pre-computed)
    """
    valid_key_constraint = f"`{primary_key_field}` IS NOT NULL"

    @dlt.table(
        name=dlt_table_name,
        comment=catalog_comment,
        table_properties={
            "delta.columnMapping.mode": "name"
        }
    )
    @dlt.expect_or_drop("valid_key", valid_key_constraint)
    def dlt_ingestion_execution():
        return (
            spark.read
            .format("csv")
            .option("header", "true")
            .schema(dlt_schema)  
            .load(source_file_path)

            # Consistent audit trace metadata
            .withColumn("audit_insert_ts", F.current_timestamp())
            .withColumn("audit_source_nm_txt", F.col("_metadata.file_path"))
            .withColumn("audit_source_system_nm", F.lit(source_system))
        )
    
    return dlt_ingestion_execution


# =====================================================================
# DAG REGISTRATION
# =====================================================================
# Register each table configuration explicitly to build the DLT graph (using tuple unpacking)

brz_asset_quotations = create_bronze_pipeline(*source_lst[0])
brz_dim_asset = create_bronze_pipeline(*source_lst[1])
brz_dim_company = create_bronze_pipeline(*source_lst[2])
brz_dim_wrapper = create_bronze_pipeline(*source_lst[3])
brz_equateplus_statement = create_bronze_pipeline(*source_lst[4])
brz_oneview_statement = create_bronze_pipeline(*source_lst[5])
brz_vanguard_isa_cash_statement = create_bronze_pipeline(*source_lst[6])
brz_vanguard_isa_transactions_statement = create_bronze_pipeline(*source_lst[7])
brz_vanguard_pension_cash_statement = create_bronze_pipeline(*source_lst[8])
brz_vanguard_pension_transactions_statement = create_bronze_pipeline(*source_lst[9])
