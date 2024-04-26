# ETL_car_shop

DESCRIPTION

The script is an ETL pipeline that extracts car data from different source files:
- CSV
- JSON
- XML

And put all data together on a single CSV file for further use or dump into a DB.
Also to generate a log file for the process

## WORKFLOW

1.- Extracts data from all CSV, JSON and XML files using "pandas" and "xml" APIs

2.- Transformed data to match format requirements(price USD to MXN)

3.- Loaded the transformed data into a single CSV file for stakeholders usage

4.- Generated a log file for the ETL process

This can be improved by adding another step to dump all this data into a DB
