# DATA PIPELINES WITH AIRFLOW

The project "Data Pipelines with Airflow" consists in a DAG that executes a ETL between a S3 server and a Redshift cluster. There is also a set of queries to create the necessary tables on the cluster.

## 1. Purpose of the database

### Sparkify the startup

Sparkify is a fictional startup for music streaming where the user can select which song to play from their favourite artists. The app is currently working and there is already information of the songs, artists, users and sessions stored in different JSONs.  

### Purpose of the database

The analytical team wants to get information of what songs users are listening to, but the current storage of information makes impossible to process that information. To solve that, the purpose of the database and ETL is to collect the song data and the logs, cross it and store it in a Redshift database, where it could be consulted by the analytics teams in order to get the KPIs that they need. 

## 2. Airflow and the ETL

Airflow is an open-source platform to execute workflows automatically. In this project, the workflow consists of a ETL with the following steps:
	- Stages the data: The ETL executes a COPY query to get the data from the JSONs to two staging tables, one for logs and one for songs.
    - Creates the fact table: Crossing the information from both staging tables, the ETL transforms the data to a fact table with the logs, called songplays.
    - Creates several dimension tables: The ETL transforms the data from the staging tables to the following dimensions: Users, Artists, Songs and Time.
    - Runs quality checks: The final step is checking if the data has good quality. In this case, as an example, the quality check tests if there is more than 2 user levels.

## 3. Use

Before setting up Airflow, a Redshift cluster must be created, and the queries in "create_tables.sql" must be executed on the query editor.
After that, the Airflow web server must be set up. In the Udacity workspace is done executing "/opt/airflow/start.sh" on the terminal. After that step is completed, you can open Airflow's UI.
In the UI, under the Admin tab, two Connections must be set, first one with the name "aws_credentials" to access the S3 where the information is stored, and second one "Redshift" with the cluster's information.
Finally, set the DAG as ON.