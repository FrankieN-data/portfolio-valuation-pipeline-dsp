# Investment Portfolio Valuation Pipeline

An enterprise-grade data engineering project implementing a medallion architecture (Bronze → Silver → Gold) for investment portfolio analytics on Databricks using Unity Catalog and Lakeflow Spark Declarative Pipelines.

## Project Overview

This project demonstrates end-to-end data engineering best practices for managing multi-source investment portfolio data, including:

* Market data (asset quotations)
* Institutional registry (companies, assets)
* Customer profiles
* Financial statements from multiple providers (EquatePlus, OneView, Vanguard)
* Tax wrapper classifications (ISA, SIPP)

## Architecture

### Medallion Pattern (3-Layer)

```
┌─────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw)                        │
│                 main.investment_portfolio_raw                    │
│                                                                   │
│  • 11 tables from parquet files                                  │
│  • Immutable raw data persistence                                │
│  • Source: /Volumes/main/investment_portfolio_raw/landing_files/ │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER (Curated)                      │
│                main.investment_portfolio_curated                 │
│                                                                   │
│  • Data cleansing & validation                                   │
│  • Type casting & standardization                                │
│  • Deduplication & quality checks                                │
└─────────────────────────────────────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│                      GOLD LAYER (Refined)                        │
│                main.investment_portfolio_refined                 │
│                                                                   │
│  • Business-ready aggregations                                   │
│  • KPIs & metrics                                                │
│  • Dashboard-optimized views                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
pipeline-portfolio-valuation/
├── README.md                           # This file
├── investment_portfolio_setup.sql      # Infrastructure setup (schemas, volumes)
├── pipeline_config.yml                 # Pipeline configuration (versioned)
│
├── transformations/                    # Pipeline transformation code
│   ├── 01_landing_to_raw.py           # Bronze: Parquet → Raw tables (✅ Complete)
│   ├── 02_raw_to_curated.py           # Silver: Cleansing & validation (TODO)
│   └── 03_curated_to_refined.py       # Gold: Aggregations & metrics (TODO)
│
├── explorations/                       # Ad-hoc analysis notebooks
│
├── _dashboards/                        # Dashboard definitions
│
├── _resources/                         # Additional configs & documentation
│
└── deployment/                         # CI/CD configs (Databricks Asset Bundles)
```

## Data Sources

### Market Data (1 file)
* **asset_quotations.parquet** - Historical asset prices and market valuations

### Dimensions (4 files)
* **dim_asset.parquet** - Asset master data (ISIN, symbols, types)
* **dim_company.parquet** - Institutional company registry
* **dim_customer.parquet** - Portfolio owner profiles
* **dim_wrapper.parquet** - Tax wrapper classifications (ISA, SIPP)

### Financial Statements (6 files)
* **equateplus_statement.parquet** - Corporate equity & share plans
* **oneview_statement.parquet** - Consolidated positions
* **vanguard_isa_cash_statement.parquet** - ISA cash ledger
* **vanguard_isa_transactions_statement.parquet** - ISA transactions
* **vanguard_pension_cash_statement.parquet** - Pension cash ledger
* **vanguard_pension_transactions_statement.parquet** - Pension transactions

## Setup Instructions

### Prerequisites
* Databricks workspace with Unity Catalog enabled
* Access to `main` catalog
* Permissions: `CREATE SCHEMA`, `CREATE VOLUME`, `CREATE TABLE`

### 1. Infrastructure Setup

Run the setup script to create schemas and volumes:

```sql
-- Execute: investment_portfolio_setup.sql
-- Creates:
--   • main.investment_portfolio_raw (Bronze schema)
--   • main.investment_portfolio_curated (Silver schema)
--   • main.investment_portfolio_refined (Gold schema)
--   • Volume: main.investment_portfolio_raw.landing_files
```

### 2. Upload Source Data

Upload parquet files to Unity Catalog volume:

```
Navigate to: Catalog → main → investment_portfolio_raw → landing_files
Upload all 11 parquet files to this volume
```

Or via CLI:
```bash
databricks fs cp *.parquet /Volumes/main/investment_portfolio_raw/landing_files/
```

### 3. Configure Pipeline

Pipeline configuration (documented in `pipeline_config.yml`):

```yaml
name: portfolio_valuation_pipeline
catalog: main
schema: investment_portfolio_raw
serverless: true
photon: true
continuous: false
development: false
```

### 4. Run Pipeline

```
Navigate to: Workflows → Pipelines → portfolio_valuation_pipeline
Click: "Run Pipeline"
```

## Bronze Layer Tables

After running `01_landing_to_raw.py`, the following tables are created:

| Table Name | Description | Source File |
|------------|-------------|-------------|
| `brz_asset_quotations` | Historical asset prices | asset_quotations.parquet |
| `brz_dim_asset` | Asset master dimension | dim_asset.parquet |
| `brz_dim_company` | Company registry | dim_company.parquet |
| `brz_dim_customer` | Customer profiles | dim_customer.parquet |
| `brz_dim_wrapper` | Tax wrapper classifications | dim_wrapper.parquet |
| `brz_equateplus_statement` | EquatePlus statements | equateplus_statement.parquet |
| `brz_oneview_statement` | OneView consolidated positions | oneview_statement.parquet |
| `brz_vanguard_isa_cash_statement` | Vanguard ISA cash ledger | vanguard_isa_cash_statement.parquet |
| `brz_vanguard_isa_transactions_statement` | Vanguard ISA transactions | vanguard_isa_transactions_statement.parquet |
| `brz_vanguard_pension_cash_statement` | Vanguard Pension cash ledger | vanguard_pension_cash_statement.parquet |
| `brz_vanguard_pension_transactions_statement` | Vanguard Pension transactions | vanguard_pension_transactions_statement.parquet |

## Technology Stack

* **Platform**: Databricks on AWS
* **Compute**: Serverless with Photon acceleration
* **Data Format**: Delta Lake (Unity Catalog managed tables)
* **Orchestration**: Lakeflow Spark Declarative Pipelines (formerly DLT)
* **Storage**: Unity Catalog Volumes
* **Language**: Python (PySpark)

## Development Roadmap

### ✅ Phase 1: Bronze Layer (Complete)
- [x] Infrastructure setup (schemas, volumes)
- [x] Raw data ingestion from parquet files
- [x] 11 bronze tables created
- [x] Data type validation

### 🚧 Phase 2: Silver Layer (In Progress)
- [ ] Data quality checks & expectations
- [ ] Type casting & standardization
- [ ] Deduplication logic
- [ ] SCD Type 2 for dimensions
- [ ] Data validation rules

### 📋 Phase 3: Gold Layer (Planned)
- [ ] Portfolio valuation metrics
- [ ] Performance analytics
- [ ] Risk metrics
- [ ] Time-series aggregations
- [ ] Dashboard-ready views

### 📋 Phase 4: Visualization & Reporting (Planned)
- [ ] Lakeview dashboards
- [ ] Executive summary views
- [ ] Portfolio performance reports

### 📋 Phase 5: Automation (Planned)
- [ ] CI/CD with Databricks Asset Bundles
- [ ] Automated testing
- [ ] Scheduled pipeline runs
- [ ] Data quality monitoring

## Best Practices Implemented

* ✅ **Medallion Architecture** - Clear separation of Bronze/Silver/Gold layers
* ✅ **Unity Catalog** - Centralized governance & access control
* ✅ **Modular Design** - One file per layer for maintainability
* ✅ **Infrastructure as Code** - SQL setup scripts versioned in Git
* ✅ **Enterprise Naming** - Consistent `brz_*`, `slv_*`, `gld_*` prefixes
* ✅ **Documentation** - Inline comments & README
* ✅ **Version Control** - Git-ready project structure

## Contributing

This is a portfolio project by **Francine Nzuzi** demonstrating data engineering expertise on Databricks.

## License

This project is for portfolio demonstration purposes.

## Contact

* **Author**: Francine Nzuzi
* **Email**: francine.nzuzi@gmail.com
* **Project Date**: May 2026

---

**Note**: This project uses synthetic/sample data for demonstration purposes. No actual financial data is included in this repository.
