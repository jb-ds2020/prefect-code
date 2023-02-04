from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0,1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(f"Columns: {df.columns}")
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as a parquet file"""

    output_dir = Path(f"data/{color}")

    output_file = f"{dataset_file}.parquet"
    output_dir.mkdir(parents=True, exist_ok=True)

    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(output_dir / output_file, compression="gzip")
    print(f"path to file: {path}")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to google cloud storage"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=f"{path}", to_path=path)

    return


@flow()
def etl_web_to_gcs() -> None:
    """The main etl function"""
    color = "green"
    year = 2020
    month = 1

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)

    path = write_local(df_clean, color, dataset_file)

    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
