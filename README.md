# nyc_taxi_end_to_end

## Tech Stack

* Azure Data Factory (ADF)
* Azure Data Lake Gen2
* Azure Databricks (Unity Catalog)
* Delta Lake

##  Architecture

* Bronze → Raw Data
* Silver → Cleaned Data
* Gold → Business Aggregates

##  Project Structure

* adf/ → ADF pipelines (ARM templates)
* databricks/ → Transformation notebooks

## Features

* End-to-end ETL pipeline
* Medallion Architecture
* Unity Catalog implementation

## Data Flow

ADF → ADLS (Bronze) → Databricks (Silver/Gold)
