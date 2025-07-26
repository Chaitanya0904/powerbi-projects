Pipeline: `GitHub_To_AzureSQL`
Pipeline-Level Parameters

| Parameter Name   | Type   | Description                                     | Example Value      |
|------------------|--------|-------------------------------------------------|---------------------|
| `platform`       | String | E-commerce platform name (used in file path)   | `amazon`, `flipkart` |
| `category`       | String | Product category (used in file name/table name) | `Smart Watches`     |



 Source File Path

@concat('Data%20Scraping/', pipeline().parameters.platform, '/', pipeline().parameters.platform, '_', replace(pipeline().parameters.category, ' ', '_'), '.csv')

 Sink Table Name

@concat('raw_', pipeline().parameters.platform, '_', pipeline().parameters.category)

🔹 Pipeline 2: AzureSQL_To_ADLS

 Source Schema Name

@item().table_schema

 Source Table Name

@item().table_name

 Sink Directory Name

@if(contains(item().TABLE_NAME, 'amazon'), 'amazon', 'flipkart')

 Sink File Name

@concat(item().TABLE_SCHEMA, '_', item().TABLE_NAME, '.csv')




