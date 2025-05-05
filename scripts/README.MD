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
  - [`[stage 03]` predictive data analysis](#stage-03-predictive-data-analysis)
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

- <sup>[1]</sup>: Actually, 2025 year data was recently published, however, it is still incomplete, thats why we are focusing on data from 2014 to 2024 years.
- <sup>[2]</sup>: Most of the rides are ordered by a single person, however, it is common for a group to share a ride, therefore, the real number of passengers could be obtained only from drivers' reports.

### Dataset Organization

In order to make the dataset accessible even trough some time and mitigate the problem of dynamically generated download URLs, we have organized our copy of the dataset. To do so, we have created an [Yandex Cloud Object Storage](https://yandex.cloud/en/services/storage) (AWS S3 Bucket) in our own cloud account. We have setup this bucket such way, so it's content publicly available for download, however manipulation actions are limited to our team members. We have also setup a website for hosting generated index of the dataset, which is available at the following URLs:
1. [bigdata.inno.dartt0n.ru](https://bigdata.inno.dartt0n.ru)
2. [dartt0n.website.yandexcloud.net](https://dartt0n.website.yandexcloud.net/ibd/index.html)

You can read more about creating index of the dataset in the corresponding [stage01](./stage01/readme.md) section or in the [stage01/index](./stage01/index/readme.md) script description.

### Dataset Download

After creating the copy of the dataset in our our Cloud Object Storage, we have implemented a script, which downloads all files from the bucket and stores them in the local file system for further processing.

You can read more about dataset download in the corresponding [stage01](./stage01/readme.md) section or in the [stage01/download](./stage01/download/readme.md) script description.

### Dataset Transformation

Unfortunately, during a long period of time (10 years from 2014 to 2024) is is natural for data schema to change. This is what happened in our case. Some of the fields changed their data types, some of them upgraded from int32 to int64 to handle larger values, some of them were converted to floating point numbers (int32 -> float32), some of them stated to contain null values. To mitigate this problem, we have tried several approaches:
- Spark dataframe Schema Merge (failed)
- Spark dataframe dynamic type casts (failed)
- Polars dataframe Schema Merge (failed)
- Polars Relaxed dataframe Concatenation (succeeded but can be executed only on a single node)
- Spark distributed dataframe conversion + merge as separate jobs (succeeded, our final solution)

You can read more about dataset transformation in the corresponding [stage01](./stage01/readme.md) section or you can either explore polars approach in the [stage01/merge](./stage01/merge/readme.md) script description or you can go directly to the [stage01/dataloader/readme.md] spark implementation.

### Dataset Partitioning and Loading

After transformation, we have applied several partitioning strategies, loaded dataset to PostgesSQL and Hive database.
Regarding partitioning, we have partitioned our data in the most common way, by year and month, creating the following partitions in HDFS:
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
Right after the partitioning the data, we have loaded it to the PosgreSQL database using Spark PosgreSQL connector. You can find the SQL Table description in the [create-table-psql.sql](../sql/create-table-psql.sql) file.

After the data is stored in PostgreSQL, we used `sqoop` tool to import the data from PostgreSQL to Hive for further processing.

You can read more about partitioning process in the corresponding [stage01](./stage01/readme.md) section or you can directly explore [stage01/dataloader/readme.md] spark implementation. PostgreSQL and Hive are discussed in the corresponding [stage01](./stage01/readme.md) section.

### `stage1.sh` script technical details

Script execution starts from preparing the logging process. It defines variables `GREEN` and `NC` for coloring the output (`GREEN` is used to start highlighting the output with green color and `NC` is used to reset the color), as well as `log` function. `log` function prints the given message in green color.

Then, scripts retrieves `SCRIPTS` and `PROJECT_ROOT` variables. To make scripts launches consistent, we need to utilize absolute path to all the parts of the project. Therefore, we have defined `SCRIPTS` variable, which is the absolute path to the `scripts` directory and `PROJECT_ROOT` variable, which is the absolute path to the root directory of the project.

After that, script loads secrets using `load-secrets.sh` script. This script reads content of the files in the `secrets/` directory and loads them into the environment variables. E.g. file `secrets/POSTGRES_HOST` would be loaded as `POSTGRES_HOST` variable.

Then, `stage1.sh` invokes `prepare-bin.sh` script, which downloads all the required binaries and saves them in the `$SCRIPTS/bin/` directory. This directory would be used to keep all the binaries and external dependencies of the script. Most notably, `prepare-bin.sh` downloads `uv` and `sbt` binaries, which are required for running the scripts.

Then, the script invokes `download-sources.py` script, which downloads all the dataset files from the source and saves them in the `$PROJECT_ROOT/data/` directory.

After downloading the dataset files into local file system, the script setups HDFS and migrates the data to the cluster, to make it available for all cluster nodes

Finally, the scripts builds `dataloadeer` scala project and invokes `spark-submit` to load the data into PostgreSQL, creating table using `create_tables.py` script beforehand.

As the very last step, `stage1.sh` invokes `sqoop` tool to import the data from PostgreSQL to Hive for further processing.

## `[stage 02]` data storage, preparation and exploration

## `[stage 03]` predictive data analysis

## `[stage 04]` data visualization