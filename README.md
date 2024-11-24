# DataFlow Insights

DataFlow Insights is an automated data pipeline project designed to streamline data ingestion, processing, and visualization using AWS services. This project pushes daily data to Amazon S3, automatically crawls and catalogs it using AWS Glue, queries the data through Amazon Athena, and finally visualizes it in Amazon QuickSight.

## Architecture

![Architecture-Diagram](https://github.com/user-attachments/assets/b799eb54-8bf7-470d-a984-69f161f1a0d4)

## Workflow

1. **Data Ingestion**: A Python script pushes daily data to an Amazon S3 bucket.
2. **Data Crawling and Cataloging**: An AWS Glue crawler identifies the data structure in S3, extracts metadata, and defines table schemas in the AWS Glue Data Catalog.
3. **Data Querying**: Amazon Athena queries the cataloged data, making it easily accessible.
4. **Data Visualization**: Amazon QuickSight connects to Athena, allowing you to create visual reports and dashboards.
