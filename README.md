# Udacity Data Engineering Nanodegree Capstone Project

## Project Summary

## Data Source Analysis

### I94 Immigration Data

This data comes from the U.S. National Tourism and Trade Office. A data dictionary is included in the workspace. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from.

This data is stored as a set of SAS7BDAT files. SAS7BDAT is a database storage file created by Statistical Analysis System (SAS) software to store data. It contains binary encoded datasets used for advanced analytics, business intelligence, data management, predictive analytics, and more. The SAS7BDAT file format is the main format used to store SAS datasets.

The immigration data is partitioned into monthly SAS files. Each file is around 300 to 700 MB. The data provided represents 12 months of data for the year 2016. This is the bulk of the data used in the project.

A data dictionary [I94_SAS_Labels_Descriptions.SAS] was provided for the immigration data. In addition to descriptions of the various fields, the port and country codes used were listed in table format. I extracted the port codes to a file i94_ports.csv. I extracted the country codes to a file i94_countries.csv. These files were placed in the data lake to be used as a lookup when extracting the immigration data.

The SAS format can be read fairly easily with pandas in python or Apache Spark.

Read SAS data with pandas example:

```python
import pandas as pd
fname = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
df = pd.read_sas(fname, 'sas7bdat', encoding="ISO-8859-1")
```

In order to load this data into Apache Spark, I used the following JAR packages:

* Spark-sas7bdat-2.1.0-s_2.11
  * https://github.com/saurfang/spark-sas7bdat
* Parso-2.0.11
  * https://mvnrepository.com/artifact/com.epam/parso/2.0.11

Reading SAS data with Spark:

```python
df = spark.read.format("com.github.saurfang.sas.spark").load(input_path)
```

Since this dataset is fairly large and was provided on an attached disk in a JupyterLab environment provided by Udacity, I decided to preprocess it with PySpark in that environment and load it to Amazon S3. I created a PySpark script to extract the SAS data and write to Parquet format. The Parquet data is partitioned by year, month, and day based on the arrival date of the immigrant. This breaks the monthly SAS immigration data down into easier to manage partitions which can be later backfilled through the Apache Airflow pipeline on a daily cadence.

### World Temperature Data

This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

With this data set I used two sources:

* GlobalLandTemperaturesByCity.csv (508 MB)
* GlobalLandTemperaturesByCountry.csv (21.6 MB)

These files contain average temperature data for countries and cities between 1743-11-01 and 2013-09-01. For the city data, I only pulled cities in the United States since we are interested in immigration through U.S. ports. This temperature data would be applied to the port cities extracted from the immigration data. Since the data all fell prior to 2016, I only pulled the latest entry for each city where a temperature was recorded.

Similarly, with the country data I pulled the latest entry that had temperature data for each country. This country data would be applied to the immigration data country of origin.

Both sets of data were extracted one time using Apache Spark and placed in a data lake in Amazon S3 in Parquet format for later processing.

### U.S. City Demographic Data

This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

This data contains demographic data for U.S. cities. This data placed in the data lake as a single CSV file (246 KB). This data will be combined with port city data to provide ancillary demographic info for port cities.

### Airport Code Table

This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data).

This data is a single CSV file (5.8 KB). It provides additional information for airports and can be combined with the immigration port city info.

## Data Model

## Data Pipeline

The main data pipeline uses Apache Airflow to process immigration data for  single day at a time. In brings in the immigration data from Amazon S3 and combines it with other staging data for ports, airport data, countries, city and country temperatures, and city demographics.

Airflow uses directed acyclic graphs (DAG's) to describe a pipeline workflow. Each DAG is made up of tasks which are the nodes of the graph. Each task implements an operator of some type to execute code.

Ultimately I wanted to use Amazon Redshift as the data warehouse, but in order to avoid the cost of Redshift I did most of the early development against a local copy of PostgreSQL.

I created four DAG's, two for Postgres and two for Amazon Redshift:

* setup_postgres.py -  This DAG is responsible for creating the schema within a Postgres database and loading some static staging data.
* setup_redshift.py -  This DAG is responsible for creating the schema within an Amazon Redshift database and loading some static staging data.

* import_i94_postgres.py -  This DAG loads the immigration data into a staging table in Postgres and then combines it with other staging data to produce the final dimension and fact table entries that are inserted.
* import_i94_redshift.py -  This DAG loads the immigration data into a staging table in Redshift and then combines it with other staging data to produce the final dimension and fact table entries that are inserted.

## Data Quality Checks

## Data Dictionary

