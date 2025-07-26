#  Competitive Pricing Intelligence Tracker

A full-stack data engineering and analytics project that scrapes product data from Flipkart & Amazon, stores and transforms it in Snowflake, and visualizes insights in Power BI.

---

##  Workflow Overview

### 1. **Data Scraping**
Product data such as title, brand, price, ratings, and reviews is scraped from Flipkart and Amazon using a Beautifulsoup Python script. The extracted data is saved as CSV files and uploaded to GitHub. These files serve as the raw input source for the ETL pipelines.

### 2. **ETL Pipelines with Azure Data Factory**
Two parameterized pipelines are created using ADF:

- **Pipeline 1 (GitHub to Azure SQL):** Copies raw CSV files from GitHub into staging tables in Azure SQL Database using the Web activity.
- **Pipeline 2 (Azure SQL to Azure Data Lake):** Extracts records from Azure SQL and stores them in Azure Data Lake Storage in clean `.csv` format, organized by platform (Amazon/Flipkart).

Both pipelines are dynamic, reusable, and configured with pipeline-level parameters.

### 3. **Snowflake Integration**
Snowpipe is set up to automatically load files from Azure Blob into the Snowflake `raw` table using Azure Event Grid notifications. This allows for incremental ingestion as new data arrives. A stored procedure is used to apply business logic and transformations on the data.

### 4. **Azure Functions**
A Python-based Azure Function is deployed via Visual Studio Code. It is triggered by an HTTP request and invokes the Snowflake stored procedure. This serverless architecture enables modular and on-demand execution of transformations.

### 5. **Power BI Dashboard**
The transformed data from the Snowflake Data Mart is used as a live source in Power BI. The dashboard presents KPIs, product pricing trends, vendor comparisons, and insights to help understand the competitive landscape.

---




