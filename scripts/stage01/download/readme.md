# `download_sources.py` script

- [`download_sources.py` script](#download_sourcespy-script)
  - [Script Arguments](#script-arguments)
  - [Script Execution](#script-execution)
  - [Used Libraries](#used-libraries)
  - [Downloading files](#downloading-files)


This is simple, but very powerful script, since it utilizes fast asynchronous parallel downloading of the files from the source. The script simply downloads all the files from the provided URL and range and saves them in the specified directory. 

Firstly, script generates a list of all the files to be downloaded. Then, it spawns a coroutine for each file, which downloads the file using streaming API and saves it in the specified directory. All the coroutines are executed in parallel, and the script waits for all of them to finish. In order not to flood the network and avoid rate limiting, the script uses a semaphore to limit the number of concurrent downloads.

## Script Arguments

- `--base-url` - base URL for downloading files
- `--max-concurrent` - maximum number of concurrent downloads
- `--start-year` - start year for data download
- `--end-year` - end year for data download (inclusive)
- `--start-month` - start month for data download
- `--end-month` - end month for data download (inclusive)
- `--file-prefix` - prefix for downloaded files
- `--file-extension` - file extension for downloaded files
- `--output-dir` - directory to save downloaded files

## Script Execution

```bash
uv run download/download_sources.py \
    --base-url https://storage.yandexcloud.net/dartt0n/ibd/ \
    --start-year 2014 \
    --end-year 2024 \
    --start-month 1 \
    --end-month 12 \
    --file-prefix green_tripdata \
    --file-extension parquet \
    --output-dir ./data
```

## Used Libraries

- [argparse](https://docs.python.org/3/library/argparse.html) - for parsing script arguments
- [httpx](https://www.python-httpx.org/) - for downloading files

## Downloading files

The script downloads all the files from the provided URL and range and saves them in the specified directory. The script uses the following algorithm:
1. Generates a list of all the files to be downloaded
2. Spawns a coroutine for each file
3. Each coroutine locks the semaphore
4. Opens the bytes stream for the file
5. Reads the file in chunks and writes them to the output file
6. Releases the semaphore
7. Application waits for all the coroutines to finish

The script constructs the URL for each file using the following algorithm:
```python
base_url + file_prefix + "_{year:04d}-{month:02d}." + file_extension
```

For example, if the base URL is `https://storage.yandexcloud.net/dartt0n/ibd/`, the file prefix is `green_tripdata`, the file extension is `parquet`, and the start year is 2014 and the end year is 2014, then the script will download the following files:
- `green_tripdata_2014-01.parquet`
- `green_tripdata_2014-02.parquet`
- `green_tripdata_2014-03.parquet`
- `green_tripdata_2014-04.parquet`
- `green_tripdata_2014-05.parquet`
- `green_tripdata_2014-06.parquet`
- `green_tripdata_2014-07.parquet`
- `green_tripdata_2014-08.parquet`
- `green_tripdata_2014-09.parquet`
- `green_tripdata_2014-10.parquet`
- `green_tripdata_2014-11.parquet`
- `green_tripdata_2014-12.parquet`