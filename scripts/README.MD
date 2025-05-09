# scripts

- [scripts](#scripts)
  - [`[stage 01]` data collection and injection](#stage-01-data-collection-and-injection)
    - [Dataset Description](#dataset-description)
    - [Dataset Organization](#dataset-organization)
    - [Dataset Download](#dataset-download)
    - [Dataset Transformation](#dataset-transformation)
    - [Dataset Partitioning and Loading](#dataset-partitioning-and-loading)
    - [`stage1.sh` script technical details](#stage1sh-script-technical-details)
  - [`[stage 02]` data storage, preparation and exploration](#stage-02-data-storage-preparation-and-exploration)
    - [Prepare data and Hive dataset](#prepare-data-and-hive-dataset)
    - [Create queries](#create-queries)
      - [Calculate missing values percentage](#calculate-missing-values-percentage)
      - [Calculate invalid rows percentage](#calculate-invalid-rows-percentage)
      - [Calculate correlations between price and other features](#calculate-correlations-between-price-and-other-features)
      - [Create table with duration](#create-table-with-duration)
      - [Create table with pick-up hour and drop-off hour](#create-table-with-pick-up-hour-and-drop-off-hour)
      - [Create table with total number of records per month and year](#create-table-with-total-number-of-records-per-month-and-year)
      - [Create table with statistics by date](#create-table-with-statistics-by-date)
    - [Add queries to dashboard](#add-queries-to-dashboard)
  - [`[stage 03]` predictive data analysis](#stage-03-predictive-data-analysis)
    - [Data Preprocessing](#data-preprocessing)
      - [Encoding Categorical Features](#encoding-categorical-features)
      - [Working with Datetime](#working-with-datetime)
      - [Numerical Features](#numerical-features)
    - [Modeling](#modeling)
      - [Linear Regression](#linear-regression) 
      - [Random Forest Regression](#random-forest-regression)
      - [Gradient Boosting](#gradient-boosting)
    - [Evaluation](#evaluation) 
  - [`[stage 04]` data visualization](#stage-04-data-visualization)


This directory contains scripts related to data manipulation. More specifically, it contains scripts to download data, generate index, transform it, repartition it, load to PostgreSQL, load to Hive, analyze it, build visualizations, and more.

## `[stage 01]` data collection and injection

The scripts in this stage are responsible for collecting data from the source, transforming data into a single format, and injecting it into the database.

### Dataset Description

As our primary dataset we have selected the [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The dataset contains information about all trips taken in New York City from 2014 to 2024 years<sup>[1]</sup>. This dataset contains the following information:
- Pick-up and Drop-off location
- Trip distance and duration
- Pick-up and Drop-off time
- Itemized fares
- Payment types
- And driver-reported passengers count<sup>[2]</sup>

The dataset is available for download in the Apache Parquet format, one file per each month. Files are shared thought CloudFare network with dynamically generated URLs.

- <sup>[1]</sup>: Actually, 2025 year data was recently published, however, it is still incomplete, that's why we are focusing on data from 2014 to 2024 years.
- <sup>[2]</sup>: Most of the rides are ordered by a single person, however, it is common for a group to share a ride, therefore, the real number of passengers could be obtained only from drivers' reports.

### Dataset Organization

In order to make the dataset accessible even through some time and mitigate the problem of dynamically generated download URLs, we have organized our copy of the dataset. To do so, we have created a [Yandex Cloud Object Storage](https://yandex.cloud/en/services/storage) (AWS S3 Bucket) in our own cloud account. We have set up this bucket such way, so it's content publicly available for download, however, manipulation actions are limited to our team members. We have also set up a website for hosting generated index of the dataset, which is available at the following URLs:
1. [bigdata.inno.dartt0n.ru](https://bigdata.inno.dartt0n.ru)
2. [dartt0n.website.yandexcloud.net](https://dartt0n.website.yandexcloud.net/ibd/index.html)

You can read more about creating index of the dataset in the corresponding [stage01](./stage01/readme.md) section or in the [stage01/index](./stage01/index/readme.md) script description.

### Dataset Download

After creating the copy of the dataset in our Cloud Object Storage, we have implemented a script, which downloads all files from the bucket and stores them in the local file system for further processing.

You can read more about dataset download in the corresponding [stage01](./stage01/readme.md) section or in the [stage01/download](./stage01/download/readme.md) script description.

### Dataset Transformation

Unfortunately, during a long period of time (10 years from 2014 to 2024) it is natural for data schema to change. This is what happened in our case. Some of the fields changed their data types, some of them upgraded from int32 to int64 to handle larger values, some of them were converted to floating point numbers (int32 -> float32), some of them stated to contain null values. To mitigate this problem, we have tried several approaches:
- Spark dataframe Schema Merge (failed)
- Spark dataframe dynamic type casts (failed)
- Polars dataframe Schema Merge (failed)
- Polars Relaxed dataframe Concatenation (succeeded but can be executed only on a single node)
- Spark distributed dataframe conversion + merge as separate jobs (succeeded, our final solution)

You can read more about dataset transformation in the corresponding [stage01](./stage01/readme.md) section, or you can either explore polars approach in the [stage01/merge](./stage01/merge/readme.md) script description, or you can go directly to the [stage01/dataloader/readme.md](spark implementation).

### Dataset Partitioning and Loading

After transformation, we have applied several partitioning strategies, loaded dataset to PostgesSQL and Hive database.
We have partitioned our data in the most common way, by year and month, creating the following partitions in HDFS:
```bash
$ hdfs dfs -lsr /user/team18/project/data
/user/team18/project/data/year=2014
/user/team18/project/data/year=2014/month=01
/user/team18/project/data/year=2014/month=01/_SUCCESS
/user/team18/project/data/year=2014/month=01/part-00000-77124932-197d-4512-97e7-5cd4954bcffc-c000.snappy.parquet
/user/team18/project/data/year=2014/month=02
/user/team18/project/data/year=2014/month=02/_SUCCESS
/user/team18/project/data/year=2014/month=02/part-00000-b2c9f531-5770-4a4f-a476-f045eaec5b11-c000.snappy.parquet
/user/team18/project/data/year=2014/month=03
/user/team18/project/data/year=2014/month=03/_SUCCESS
/user/team18/project/data/year=2014/month=03/part-00000-b95218fa-b204-4166-8321-a3d03033a1fb-c000.snappy.parquet
...
```
Right after partitioning the data, we have loaded it to the PosgreSQL database using Spark PosgreSQL connector. You can find the SQL Table description in the [create-table-psql.sql](../sql/create-table-psql.sql) file.

After the data is stored in PostgreSQL, we used `sqoop` tool to import the data from PostgreSQL to Hive for further processing.

You can read more about partitioning process in the corresponding [stage01](./stage01/readme.md) section, or you can directly explore [stage01/dataloader/readme.md] spark implementation. PostgreSQL and Hive are discussed in the corresponding [stage01](./stage01/readme.md) section.

### `stage1.sh` script technical details

Script execution starts from preparing the logging process. It defines variables `GREEN` and `NC` for coloring the output (`GREEN` is used to start highlighting the output with green color and `NC` is used to reset the color), as well as `log` function. `log` function prints the given message in green color.

Then, scripts retrieves `SCRIPTS` and `PROJECT_ROOT` variables. To make scripts launches consistent, we need to utilize absolute path to all the parts of the project. Therefore, we have defined `SCRIPTS` variable, which is the absolute path to the `scripts` directory and `PROJECT_ROOT` variable, which is the absolute path to the root directory of the project.

After that, script loads secrets using `load-secrets.sh` script. This script reads content of the files in the `secrets/` directory and loads them into the environment variables. E.g. file `secrets/POSTGRES_HOST` would be loaded as `POSTGRES_HOST` variable.

Then, `stage1.sh` invokes `prepare-bin.sh` script, which downloads all the required binaries and saves them in the `$SCRIPTS/bin/` directory. This directory would be used to keep all the binaries and external dependencies of the script. Most notably, `prepare-bin.sh` downloads `uv` and `sbt` binaries, which are required for running the scripts.

Then, the script invokes `download_sources.py` script, which downloads all the dataset files from the source and saves them in the `$PROJECT_ROOT/data/` directory.

After downloading the dataset files into local file system, the script setups HDFS and migrates the data to the cluster, to make it available for all cluster nodes

Finally, the scripts builds `dataloader` scala project and invokes `spark-submit` to load the data into PostgreSQL, creating table using `create_tables.py` script beforehand.

As the very last step, `stage1.sh` invokes `sqoop` tool to import the data from PostgreSQL to Hive for further processing.

## `[stage 02]` data storage, preparation and exploration

### Prepare data and Hive dataset

First, we have to obtain avro schema from HDFS. 

Then we launch the [`sql/db.hql`](../sql/db.hql) to create external Hive table, load data from files stored in HDFS, create external partition tables by year and month and delete the original dataset, since it is no longer needed.

### Create queries

Using loaded data, we then perform queries needed for data analysis.

#### Calculate missing values percentage

[`sql/q1.hql`](../sql/q1.hql) script creates a table with percentage of null values in each column.

#### Calculate invalid rows percentage

[`sql/q2.hql`](../sql/q2.hql) script calculates number of weird records. 

We considered a record weird if one of certain problems is encountered:
- Drop-off time is before pick up time.
- Trip distance is equal to 0.
- Number of passengers is equal to 0.
- The pick-up location is the same as drop-off location.

#### Calculate correlations between price and other features

[`q3.py`](stage02/q3.py) script calculates correlation between price and a list of certain features:
- duration,
- distance,
- number of passengers.

#### Create table with duration

[`q4.py`](stage02/q4.py) script creates a table with following columns:
- year
- month
- price
- number of passengers
- distance
- duration (custom column)

The table is needed for faster graph creation.

#### Create table with pick-up hour and drop-off hour

[`q5.py`](stage02/q5.py) script creates a table with following columns:
- price
- pick-up hour
- drop-off hour

The table allows to see correlation between price and time during the day at which the trip happened.

### Create table with total number of records per month and year

[`q7.py`](q7.py) script calculates number of records in the database per every partition (for every month and year combination).
The table allows to see how number of trips changed by time.

### Create table with statistics by date

[`q8.py`](q8.py) script calculates the trip statistics for each date within a year:
- total earnings
- average price
- number of trips

The table allows us to see any seasonality trends present in the data, and see if they correlate with any significant dates.


### Add queries to dashboard

We used the created tables to create graphs in Superset, since recomputing all the values for visualization would use too much computational resources.

## `[stage 03]` predictive data analysis

This stage is responsible for training the models and performing PDA.

## Data Preprocessing

We split the data into 80% training and 20% testing dataset. We have to split data before preprocessing to avoid data leaking from test set.

Then, we initiate the data preprocessing pipeline.

### Encoding Categorical Features

Some of the features are categorical, so storing them as integer values would make no sense. Those features are:
- Vendor ID
- Ratecode ID
- Payment type
- Trip type

To encode these features, we use One Hot Encoder, and then assemble them into single vector of categorical features using Vector Assembler.

### Working with Datetime

The lpep_pickup_datetime has a datetime value. We need to transform it to numerical features. 

For this, we created a separate DateTimeTransformer class. It obtains the following values from datetime:
- Year of trip
- Month of trip, encoded with sin and cos (cyclical feature)
- Day of the week of trip, encoded with sin and cos (cyclical feature)
- Hour of trip, encoded with sin and cos (cyclical feature)
- Minute of trip, encoded with sin and cos (cyclical feature)
- Second of trip, encoded with sin and cos (cyclical feature)

### Numerical features

To provide more consistent results, numerical features have to be scaled to general range, so that no feature can influence the result only because of big value.
To perform scaling, we simply use StandardScaler.

At the end, all features are concatenated into one Vector.

## Modeling

We have created separate models to predict the fare amount. This is a regression task.

### Linear Regression

Linear Regression is a simple regression models that tries to find coefficients for each of input parameters to estimate a linear function that would predict the resulting value.

We used the following parameter grid for Linear regression:

```python
regParam: [0.01, 0.1, 1]
elasticNetParam: [0.0, 0.5, 1.0]
```

### Random Forest Regression

Random Forest Regression model attempts to find the value of result by creating multiple trees, each of those tries to predict the value based on input variables.

We used the following parameter grid for Random Forest Regression:

```python
numTrees: [25, 50, 75]
maxDepth: [3, 4, 5]
```

### Gradient Boosting

Gradient Boosting attempts to train a few weak learners using Gradient Descent, and combine them into an ansamble.

We used the following parameter grid for Gradient Boosting:

```python
maxDepth: [2, 3, 4, 5, 7]
```

## Evaluation

To evaluate the model, we used:
- Root Mean Square Error (RMSE)
- Determination coefficient (R2)

These metrics are the common ones used for evaluating regression tasks, since RMSE provides general insight on how far prediction is from average value, while R2 shows how well the model captures the determination of results.

## `[stage 04]` data visualization

Since most of the data preparation was done during previous stages, no code was needed here. 

We have simply aggregated all the insights gathered during previous stages into Apache Superset Dashboard.

We will briefly describe what each tab is about.

### Overview

The tab contains general information about the project, and example of rows from the original dataset.

### Business Insights

The tab contains insights useful for the business, with clear explanations of each graph and conclusions based on it.

### EDA

The tab contains insights gathered during Data Exploration stage, in which we gathered useful insights from the modeling point of view.

### Modeling

The tab contains results of best models of each type, as well as graphs of how predictions correspond to actual data.