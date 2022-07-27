# MongoDB to BigQuery using Google Cloud Function - ETL 
Google Cloud Function built to perform an ETL Job to Collect MongoDB Data and Transform it to be able to Import it to Bigquery. 

Google CFun is Connecting to MongoDB and Extracts documents within a specific Collection with applying update model to get only the delta of new/updated documents. And having two layers of modeling on BQ as the ODS to stage the delta only and then merge it to the data warehouse layer. 

Cloud Funtion has been written in python with 5-main functions:
  1- MAIN() -- Orchestrate the rest of the functions
  2- confReader() -- The full function is built on a separate propertes.conf file. Which has to be read at the early stages to enable the connections and properties needed. 
  3- mongoDataExtract() -- Responsible to connect to MongoDB Collection using the connection string within the configuration and grap the documents delta.
  4- dataPrep() -- It performs the data & schema validations to fit within the BQ Insert.
  5- BQTable_Insert() -- It Inserts the Table to the ODS Dataset as the delta only (Contains new/updated records) & then merge that to the DWH Layer to have a consalidated table at the end.

Script procedures are being logged step by step. 
