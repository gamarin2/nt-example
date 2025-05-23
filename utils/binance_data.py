import aiohttp
import asyncio
import zipfile
import io
from pathlib import Path
from datetime import datetime, timedelta, date
import argparse
import re
import pandas as pd


def get_existing_dates(
    local_path: Path,
    data_type: str = "klines",
    interval: str | None = None,
    symbol: str = "",
) -> set[date]:
    """
    Get a set of dates for which data files already exist.

    Args:
        local_path: Path to the directory containing data files (should include symbol and interval in path)
        data_type: Type of data ('klines' or other)

    Returns:
        Set of dates for which data files exist
    """
    # Ensure local_path is absolute
    local_path = Path(local_path).resolve()

    if data_type == "klines":
        if not interval:
            raise ValueError("interval is required for klines data_type")
        pattern = re.compile(
            rf"^{re.escape(symbol)}-{re.escape(interval)}-(\d{{4}}-\d{{2}}-\d{{2}})\.csv$"
        )
        glob_pattern = f"{symbol}-{interval}-*.csv"
    else:
        pattern = re.compile(rf"^{re.escape(symbol)}-(\d{{4}}-\d{{2}}-\d{{2}})\.csv$")
        glob_pattern = f"{symbol}-*.csv"

    existing_dates = set()
    for file in local_path.glob(glob_pattern):
        match = pattern.match(file.name)
        if match:
            try:
                date_obj = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                existing_dates.add(date_obj)
            except Exception:
                pass
        else:
            print(f"Pattern did not match: {pattern.pattern}")  # Debug print
    return existing_dates


async def download_and_save_csv(session, url, out_path, filename, semaphore, retries=3):
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                            csv_name = zip_file.namelist()[0]
                            with zip_file.open(csv_name) as csv_file:
                                with open(out_path, "wb") as f:
                                    f.write(csv_file.read())
                        print(f"Downloaded and saved {filename}")
                        return True
                    else:
                        print(f"Failed to download {filename}: {response.status}")
            except Exception as e:
                print(f"Error downloading {filename} (attempt {attempt}): {e}")
            await asyncio.sleep(1)  # brief pause before retry
        print(f"Giving up on {filename} after {retries} attempts.")
    return False


async def download_binance_data(
    symbol: str,
    start_date: str,
    end_date: str,
    base_url: str = "https://data.binance.vision/data/",
    instrument: str = "futures",
    market: str = "um",
    timeframe: str = "daily",
    data_type: str = "klines",
    interval: str | None = None,
    output_dir: str = "data/binance/",  # This will be relative to project root
    max_concurrent_downloads: int = 10,
):
    """
    Download Binance data for a specific symbol and timeframe asynchronously, mirroring the Binance API directory structure.
    """
    if data_type not in ("klines", "bookTicker"):
        raise ValueError("data_type must be 'klines' or 'bookTicker'")
    if data_type == "klines" and not interval:
        raise ValueError("interval is required for klines data_type")

    # Get the project root directory (2 levels up from the script)
    project_root = Path(__file__).parent.parent
    output_path = project_root / output_dir

    # Build local and remote paths
    if data_type == "klines" and interval:
        local_path = (
            output_path
            / instrument
            / market
            / timeframe
            / data_type
            / symbol
            / interval
        )
        remote_path = f"{base_url}{instrument}/{market}/{timeframe}/{data_type}/{symbol}/{interval}/"
    else:
        local_path = output_path / instrument / market / timeframe / data_type / symbol
        remote_path = (
            f"{base_url}{instrument}/{market}/{timeframe}/{data_type}/{symbol}/"
        )
    local_path.mkdir(parents=True, exist_ok=True)

    # Parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    existing_dates = get_existing_dates(
        local_path=local_path,
        data_type=data_type,
        interval=interval,
        symbol=symbol,
    )

    # Step 1: Prepare list of dates to download (only missing ones)
    missing_dates = []
    date = start
    while date <= end:
        if date not in existing_dates:
            missing_dates.append(date)
        date += timedelta(days=1)

    if not missing_dates:
        return

    # Step 2: Download missing days (in memory)
    semaphore = asyncio.Semaphore(max_concurrent_downloads)
    async with aiohttp.ClientSession() as session:
        tasks = []
        for date in missing_dates:
            date_str = date.strftime("%Y-%m-%d")
            if data_type == "klines":
                filename = f"{symbol}-{interval}-{date_str}.zip"
                out_filename = f"{symbol}-{interval}-{date_str}.csv"
            else:
                filename = f"{symbol}-{date_str}.zip"
                out_filename = f"{symbol}-{date_str}.csv"
            url = f"{remote_path}{filename}"
            out_path = local_path / out_filename
            if out_path.exists():
                continue
            tasks.append(
                download_and_save_csv(session, url, out_path, filename, semaphore)
            )
        if tasks:
            await asyncio.gather(*tasks)
    print("Download complete.")


async def get_combined_dataframe(
    symbol: str,
    interval: str,
    start_date: str,
    end_date: str,
    data_dir: str = "data/binance/",  # This will be relative to project root
    instrument: str = "futures",
    market: str = "um",
    timeframe: str = "daily",
    data_type: str = "klines",
    max_concurrent_downloads: int = 10,
) -> pd.DataFrame:
    # Get the project root directory
    project_root = Path(__file__).parent.parent
    data_path = project_root / data_dir

    # Step 1: Ensure all files are present (download if needed)
    await download_binance_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        base_url="https://data.binance.vision/data/",
        instrument=instrument,
        market=market,
        timeframe=timeframe,
        data_type=data_type,
        interval=interval,
        output_dir=str(
            data_path
        ),  # Convert to string since download_binance_data expects string
        max_concurrent_downloads=max_concurrent_downloads,
    )

    # Step 2: Combine daily CSVs into a DataFrame
    local_path = (
        data_path / instrument / market / timeframe / data_type / symbol / interval
    )
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    dfs = []
    date = start
    while date <= end:
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{symbol}-{interval}-{date_str}.csv"
        file_path = local_path / filename
        if file_path.exists():
            dfs.append(pd.read_csv(file_path))
        else:
            print(f"Warning: {file_path} does not exist, skipping.")
        date += timedelta(days=1)

    if not dfs:
        raise ValueError("No data found, exiting.")

    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download Binance futures data")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Trading pair")
    parser.add_argument(
        "--interval", type=str, default="1h", help="Candlestick interval"
    )
    parser.add_argument(
        "--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end_date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "--output_dir", type=str, default="data/binance/", help="Output directory"
    )
    parser.add_argument(
        "--max_concurrent_downloads",
        type=int,
        default=10,
        help="Maximum number of concurrent downloads",
    )

    args = parser.parse_args()

    asyncio.run(
        download_binance_data(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            output_dir=args.output_dir,
            max_concurrent_downloads=args.max_concurrent_downloads,
            interval=args.interval,
        )
    )
