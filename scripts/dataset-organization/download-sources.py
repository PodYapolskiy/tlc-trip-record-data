import asyncio
import httpx
import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser(description="Download NYC taxi trip data.")
    parser.add_argument(
        "--base-url",
        type=str,
        default="https://d37ci6vzurychx.cloudfront.net/trip-data/",
        help="Base URL for downloading files",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=10,
        help="Maximum number of concurrent downloads",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2022,
        help="Start year for data download",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2024,
        help="End year for data download (inclusive)",
    )
    parser.add_argument(
        "--start-month",
        type=int,
        default=1,
        help="Start month for data download",
    )
    parser.add_argument(
        "--end-month",
        type=int,
        default=12,
        help="End month for data download (inclusive)",
    )
    parser.add_argument(
        "--file-prefix",
        type=str,
        default="yellow_tripdata",
        help="Prefix for downloaded files",
    )
    parser.add_argument(
        "--file-extension",
        type=str,
        default="parquet",
        help="File extension for downloaded files",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data",
        help="Directory to save downloaded files",
    )
    return parser.parse_args()


async def download_and_save(filepath, base_url, output_dir, semaphore):
    async with semaphore:
        full_path = os.path.join(output_dir, filepath)
        with open(full_path, "wb") as f:
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", base_url + filepath) as r:
                    async for chunk in r.aiter_bytes():
                        f.write(chunk)
        print(f"downloaded: {filepath}")


async def main():
    args = parse_args()

    base = args.base_url
    semaphore = asyncio.Semaphore(args.max_concurrent)

    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    jobs = []

    for year in range(args.start_year, args.end_year + 1):
        for month in range(args.start_month, args.end_month + 1):
            file = f"{args.file_prefix}_{year:04d}-{month:02d}.{args.file_extension}"
            jobs.append(download_and_save(file, base, args.output_dir, semaphore))

    await asyncio.gather(*jobs)


if __name__ == "__main__":
    asyncio.run(main())
