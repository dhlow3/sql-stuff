# sql-stuff

> Set up MSSQL connection with sqlalchemy and execute SQL code.

## Tested On
1. Windows 7 Professional
2. Python 3.5.2

## Requirements
* os
* pandas
* sqlalchemy

## Setup
* the SQLConnection class requires a server and database specified for initialization
* the SQLConnection class uses the standard SQL+Server+Native+Client+11.0 driver

## Usage
1. Initialize a SQLConnection object
2. Execute sql from a .sql file path or from a SQL statement written out in a string
3. To return the SQL results in a pandas DataFrame, set return_results=True in the execute_sql method