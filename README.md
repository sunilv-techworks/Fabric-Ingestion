# Fabric-Ingestion

Medallion Architecture for D365 F&O data exported as CSV file

Source - D365 F&O data exported as CSV files using Synapse link
Bronze - CSV Files converted to Parquet using Notebooks/Pipelines/Variables
Silver - MLV to transform the Bronze
