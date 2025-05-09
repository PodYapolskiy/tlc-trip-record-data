# `[stage02]`

- [`[stage02]`](#stage02)
  - [File Structure](#file-structure)
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

This stage is responsible for performing the Exploratory Data Analysis.

## File Structure

- `readme.md` - you are here.
- [`q3.py`](q3.py) - SparkSQL script for third query.
- [`q4.py`](q4.py) - SparkSQL script for forth query.
- [`q5.py`](q5.py) - SparkSQL script for fifth query.
- [`q6.py`](q6.py) - Deprecated script for calculating price depending on pick-up and drop-off locations.
- [`q7.py`](q7.py) - SparkSQL script for seventh query.
- [`q8.py`](q8.py) - SparkSQL script for eighth query.
- [`requirements.txt`](requirements.txt) - file with Python packages needed for Python scripts.

## Prepare data and Hive dataset

First, we have to obtain avro schema from HDFS. 

Then we launch the [`sql/db.hql`](../../sql/db.hql) to create external Hive table, load data from files stored in HDFS, create external partition tables by year and month and delete the original dataset, since it is no longer needed.

## Create queries

Using loaded data, we then perform queries needed for data analysis.

### Calculate missing values percentage

[`sql/q1.hql`](../../sql/q1.hql) script creates a table with percentage of null values in each column.

### Calculate invalid rows percentage

[`sql/q2.hql`](../../sql/q2.hql) script calculates number of weird records. 

We considered a record weird if one of certain problems is encountered:
- Drop-off time is before pick up time.
- Trip distance is equal to 0.
- Number of passengers is equal to 0.
- The pick-up location is the same as drop-off location.

### Calculate correlations between price and other features

[`q3.py`](q3.py) script calculates correlation between price and a list of certain features:
- duration,
- distance,
- number of passengers.

### Create table with duration

[`q4.py`](q4.py) script creates a table with following columns:
- year
- month
- price
- number of passengers
- distance
- duration (custom column)

The table is needed for faster graph creation.

### Create table with pick-up hour and drop-off hour

[`q5.py`](q5.py) script creates a table with following columns:
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

## Add queries to dashboard

We used the created tables to create graphs in Superset, since recomputing all the values for visualization would use too much computational resources.