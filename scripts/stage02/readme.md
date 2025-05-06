# `[stage02]`

This stage is responsible for performing the Exploratory Data Analysis.

## File Structure

- `readme.md` - you are here.
- [`q3.py`](q3.py) - SparkSQL script fo third query.
- [`q4.py`](q4.py) - SparkSQL script fo forth query.
- [`q5.py`](q5.py) - SparkSQL script fo fifth query.
- [`q6.py`](q6.py) - SparkSQL script fo sixth query.
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
### Create table with pick-up and drop-off locations and average price

[`q6.py`](q6.py) script creates a table with average price for certain pick-up and drop-off locations, as well as year and month.

## Add queries to dashboard

We used the created tables to create graphs in Superset, since recomputing all the values for visualization would use too much computational resources.