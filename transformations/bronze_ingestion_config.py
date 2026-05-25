"""
Bronze layer ingestion configuration.

This module contains the source configuration for bronze layer CSV ingestion.
It is separated from the DLT pipeline code to enable easy testing without 
requiring DLT/Spark runtime dependencies.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# =====================================================================
# VOLUME CONFIGURATION
# =====================================================================

VOLUME_ROOT = "/Volumes/main/investment_portfolio_raw/landing_files"


# =====================================================================
# SOURCE CONFIGURATION (Single Source of Truth)
# =====================================================================
# Structure: (filename, schema, source_system, comment)

source_configuration_lst = [
    ("asset_quotations",
     StructType([
     StructField("quote_date_dt", StringType()),              
     StructField("isin", StringType()),
     StructField("asset_price_GBP_amt", DoubleType())
    ]),
     "LOCAL",
     "Daily spot close evaluation market feeds"),
    ("dim_asset",
     StructType([
     StructField("fdasst_asset_id", IntegerType()),              
     StructField("isin", StringType()),
     StructField("asset_nm", StringType()),
     StructField("asset_short_nm", StringType()),
     StructField("stock_market_nm", StringType()),
     StructField("asset_class_nm", StringType()),
     StructField("asset_type_nm", StringType()),
     StructField("asset_income_treatment_nm", StringType()),
     StructField("asset_base_currency_cd", StringType())
    ]),
     "LOCAL",
     "Raw asset dimension records"), 
    ("dim_company",
     StructType([
     StructField("company_number_key", StringType()),              
     StructField("company_number_system_cd", StringType()),
     StructField("company_country_cd", StringType()),
     StructField("firm_register_number", StringType()),
     StructField("company_name_txt", StringType()),
     StructField("company_shortname_txt", StringType())
    ]),
     "LOCAL",
     "Raw corporate entity profile registry"),
    ("dim_wrapper",
     StructType([
     StructField("wrapper_key", StringType()),              
     StructField("wrapper_name_txt", StringType()),
     StructField("wrapper_type_cd", StringType()),
     StructField("wrapper_subtype_cd", StringType()),
     StructField("tax_regime_uk_cd", StringType())
    ]),
     "LOCAL",
     "Tax wrapper configuration definitions"),
    ("equateplus_statement",
     StructType([
     StructField("Allocation date", StringType()),              
     StructField("Plan", StringType()),
     StructField("Instrument type", StringType()), 
     StructField("Instrument", StringType()),              
     StructField("Contribution type", StringType()),
     StructField("Strike price / Cost basis", DoubleType()),
     StructField("Market price", DoubleType()),
     StructField("Available from", StringType()),              
     StructField("Expiry date", StringType()),
     StructField("Allocated quantity", DoubleType()),
     StructField("Outstanding quantity", DoubleType()),
     StructField("Available quantity", DoubleType()),
     StructField("Estimated current outstanding value", DoubleType()),
     StructField("Estimated current available value", DoubleType())
    ]),
     "EQUATEPLUS",
     "EquatePlus monthly statement"), 
    ("oneview_statement",
     StructType([
     StructField("Trade Date", StringType()),              
     StructField("Transaction Type", StringType()),     
     StructField("Fund name", StringType()),
     StructField("Value", DoubleType()),
     StructField("Traded Units", DoubleType()),
     StructField("Trade Price", DoubleType()),
     StructField("Switch No.", IntegerType())
    ]),
     "ONEVIEW",
     "OneView monthly statement"),    
    ("vanguard_isa_cash_statement",
     StructType([
     StructField("Date", StringType()),              
     StructField("Details", StringType()),        
     StructField("Amount", DoubleType()),
     StructField("Balance", DoubleType())
    ]),
     "VANGUARD",
     "Vanguard ISA cash balance"),   
    ("vanguard_isa_transactions_statement",
     StructType([
     StructField("Date", StringType()),              
     StructField("InvestmentName", StringType()),                  
     StructField("TransactionDetails", StringType()),        
     StructField("Quantity", DoubleType()),
     StructField("Price", DoubleType()),
     StructField("Cost", DoubleType())
    ]),
     "VANGUARD",
     "Vanguard ISA transactions"), 
    ("vanguard_pension_cash_statement",
     StructType([
     StructField("Date", StringType()),              
     StructField("Details", StringType()),        
     StructField("Amount", DoubleType()),
     StructField("Balance", DoubleType())
    ]),
     "VANGUARD",
     "Vanguard SIPP cash balance"), 
    ("vanguard_pension_transactions_statement",
     StructType([
     StructField("Date", StringType()),              
     StructField("InvestmentName", StringType()),                  
     StructField("TransactionDetails", StringType()),        
     StructField("Quantity", DoubleType()),
     StructField("Price", DoubleType()),
     StructField("Cost", DoubleType())
    ]),
     "VANGUARD",
     "Vanguard SIPP transactions")    
]


# =====================================================================
# SOURCE LIST (Derived from source_configuration_lst)
# =====================================================================
# Structure: (source_file_path, source_system, dlt_schema, dlt_table_name, catalog_comment, primary_key_field)

source_lst = []
for config in source_configuration_lst:
    source_file_path = f"{VOLUME_ROOT}/{config[0]}.csv"
    table_name = f"brz_{config[0]}"
    table_schema = config[1] 
    primary_key_field = table_schema[0].name  
    
    source_lst.append(
        (source_file_path,   
         config[2],                          # source_system
         table_schema,                             
         table_name,                 
         config[3],                          # catalog_comment
         primary_key_field)                  
    )
