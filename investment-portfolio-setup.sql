/* =====================================================================
     1. CREATE SCHEMAS
   =====================================================================*/

-- Raw layer schema
CREATE SCHEMA IF NOT EXISTS main.investment_portfolio_raw
COMMENT 'Raw layer for investment portfolio data - landing zone for ingested files';

-- Curated layer schema
CREATE SCHEMA IF NOT EXISTS main.investment_portfolio_curated
COMMENT 'Curated layer for investment portfolio data - cleaned and validated';

-- Refined layer schema
CREATE SCHEMA IF NOT EXISTS main.investment_portfolio_refined
COMMENT 'Refined layer for investment portfolio data - aggregated and business-ready';


/* =====================================================================
     2. CREATE VOLUME - LANDING ZONE
   =====================================================================*/
CREATE VOLUME IF NOT EXISTS main.investment_portfolio_raw.landing_files
COMMENT 'Landing zone for raw parquet files';