import dlt

# =====================================================================
# 1. LANDING ZONE STORAGE PATHS
# =====================================================================
VOLUME_ROOT = "/Volumes/main/investment_portfolio_raw/landing_files"

# Market Data
PATH_ASSET_QUOTATIONS     = f"{VOLUME_ROOT}/asset_quotations.parquet"

# Core Dimensions
PATH_DIM_ASSET            = f"{VOLUME_ROOT}/dim_asset.parquet"
PATH_DIM_COMPANY          = f"{VOLUME_ROOT}/dim_company.parquet"
PATH_DIM_CUSTOMER         = f"{VOLUME_ROOT}/dim_customer.parquet"
PATH_DIM_WRAPPER          = f"{VOLUME_ROOT}/dim_wrapper.parquet"

# Financial Statements & Ledgers
PATH_EQUATEPLUS_STMT      = f"{VOLUME_ROOT}/equateplus_statement.parquet"
PATH_ONEVIEW_STMT         = f"{VOLUME_ROOT}/oneview_statement.parquet"
PATH_VANGUARD_ISA_CASH    = f"{VOLUME_ROOT}/vanguard_isa_cash_statement.parquet"
PATH_VANGUARD_ISA_TXN     = f"{VOLUME_ROOT}/vanguard_isa_transactions_statement.parquet"
PATH_VANGUARD_PENSION_CSH = f"{VOLUME_ROOT}/vanguard_pension_cash_statement.parquet"
PATH_VANGUARD_PENSION_TXN = f"{VOLUME_ROOT}/vanguard_pension_transactions_statement.parquet"


# =====================================================================
# 2. MARKET DATA INGESTION
# =====================================================================

@dlt.table(
    name="brz_asset_quotations",
    comment="Raw persistence layer for historical asset prices and market quotations"
)
def brz_asset_quotations():
    return spark.read.format("parquet").load(PATH_ASSET_QUOTATIONS)


# =====================================================================
# 3. DIMENSION INGESTION (MASTER DATA)
# =====================================================================

@dlt.table(
    name="brz_dim_asset",
    comment="Raw persistence copy of the asset master dimension data"
)
def brz_dim_asset():
    return spark.read.format("parquet").load(PATH_DIM_ASSET)


@dlt.table(
    name="brz_dim_company",
    comment="Raw persistence copy of the institutional company registry"
)
def brz_dim_company():
    return spark.read.format("parquet").load(PATH_DIM_COMPANY)


@dlt.table(
    name="brz_dim_customer",
    comment="Raw persistence copy of portfolio owner profiles"
)
def brz_dim_customer():
    return spark.read.format("parquet").load(PATH_DIM_CUSTOMER)


@dlt.table(
    name="brz_dim_wrapper",
    comment="Raw persistence copy of tax wrapper classification structures (e.g., ISA, SIPP)"
)
def brz_dim_wrapper():
    return spark.read.format("parquet").load(PATH_DIM_WRAPPER)


# =====================================================================
# 4. STATEMENT INGESTION (TRANSACTION RECORDS)
# =====================================================================

@dlt.table(
    name="brz_equateplus_statement",
    comment="Raw corporate equity and share plan statements from EquatePlus"
)
def brz_equateplus_statement():
    return spark.read.format("parquet").load(PATH_EQUATEPLUS_STMT)


@dlt.table(
    name="brz_oneview_statement",
    comment="Raw consolidated financial positions from OneView"
)
def brz_oneview_statement():
    return spark.read.format("parquet").load(PATH_ONEVIEW_STMT)


@dlt.table(
    name="brz_vanguard_isa_cash_statement",
    comment="Raw cash ledger movements within the Vanguard Stocks & Shares ISA wrapper"
)
def brz_vanguard_isa_cash_statement():
    return spark.read.format("parquet").load(PATH_VANGUARD_ISA_CASH)


@dlt.table(
    name="brz_vanguard_isa_transactions_statement",
    comment="Raw investment buy/sell transactions within the Vanguard Stocks & Shares ISA wrapper"
)
def brz_vanguard_isa_transactions_statement():
    return spark.read.format("parquet").load(PATH_VANGUARD_ISA_TXN)


@dlt.table(
    name="brz_vanguard_pension_cash_statement",
    comment="Raw cash ledger movements within the Vanguard Personal Pension wrapper"
)
def brz_vanguard_pension_cash_statement():
    return spark.read.format("parquet").load(PATH_VANGUARD_PENSION_CSH)


@dlt.table(
    name="brz_vanguard_pension_transactions_statement",
    comment="Raw investment buy/sell transactions within the Vanguard Personal Pension wrapper"
)
def brz_vanguard_pension_transactions_statement():
    return spark.read.format("parquet").load(PATH_VANGUARD_PENSION_TXN)