# Banking Pipeline — End-to-End Data Engineering Project
  ## Overview
    An end-to-end data engineering pipeline built on Databricks using the real world Berka dataset.
    A collection of financial data from a Czech bank containing over 1 million transactions across 8 relational tables. 
    The pipeline implements the Medallion architecture (Bronze/Silver/Gold) with a full star schema data model in the Gold layer.
  ## Dataset
    The Berka dataset contains real banking data including:

    1,000,000+ transactions
    5,300+ bank clients
    700+ loans
    900+ credit cards
    8 relational CSV files

## Architecture
    8 CSV files (Berka Dataset — Czech Bank)
            
    Bronze  — 8 raw Delta tables (loaded as-is)
            
    Silver  — 8 cleaned + transformed tables
            
    Gold    — 5 dimension tables + 2 fact tables (star schema)
            
    Jobs    — Databricks Workflow orchestrating full pipeline
## Tech Stack

    Platform: Databricks
    Processing: Apache Spark, PySpark
    Storage: Delta Lake, Databricks Volumes
    Language: Python, SQL
    Architecture: Medallion (Bronze/Silver/Gold)
    Data Model: Star Schema (fact + dimension tables)

## Pipeline Notebooks
    Notebook Description:
    config: Centralised config for catalog, schema, file paths
    00_pipeline_runner: Master orchestrator -> runs all layers in sequence 
    01_bronze_ingestion: Loads all 8 CSV files raw into Delta tables
    02_silver_transform: Cleans, transforms and enriches all tables
    03_gold: Builds star schema, dimensions and fact tables
### Silver Transformations

    Decoded Czech language values to English (transaction types, payment categories, loan status)
    Extracted gender and date of birth from encoded birth_number column
    Converted YYMMDD date formats to proper date types
    Handled ? placeholder nulls using try_cast
    Applied fillna strategically to preserve 1M transaction rows
    Renamed all columns for clarity 

### Gold Layer: Star Schema
    dim_clients
    dim_accounts
    dim_districts     =>    fact_transactions
    dim_cards
    dim_loans         =>     fact_orders
    
    
    Table Description:
    dim_clients: Client details with decoded birth date and gender
    dim_accounts: Account info with readable frequency labels
    dim_districts: Geographic and demographic data
    dim_cards: Credit card details linked to clients
    dim_loans: Loan details with readable status labels
    fact_transactions1M+: transaction records with client context
    fact_orders: Standing payment orders per account
    
  ## Key Concepts Demonstrated

Multi file ingestion from Databricks Volumes
Real world messy data handling (encoded values, foreign language, malformed nulls)
ELT pipeline design across Bronze/Silver/Gold layers
Star schema data modelling with fact and dimension tables
Multiple fact tables in a single schema
Pipeline orchestration with Databricks Workflows (Jobs)
Centralised config management

Notes
Built independently using a real banking dataset. Focused on demonstrating production grade data engineering patterns
including data modelling, transformation logic, and pipeline orchestration.
