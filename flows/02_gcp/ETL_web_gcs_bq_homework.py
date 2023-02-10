from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash

### Functions for web to GCS


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(f"Columns: {df.columns}")

    try:
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    except:
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


### Functions for GCS to BigQuery
@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")

    return Path(f"../data/{gcs_path}")


@task()
def read_df(path: Path) -> pd.DataFrame:
    """Data frame reading from parquet"""
    df = pd.read_parquet(path)
    return df


@task()
def write_bq(df: pd.DataFrame, color: str) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-dreds")
    
    table_name = f"dezoomcamp.{color}_rides"
    
    df.to_gbq(
        destination_table=table_name,
        project_id="ringed-enigma-376110",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


### Subflows here
@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """gets the data from web and stores it into gcs"""

    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    print(f"Name of the file: {dataset_file}")
    print(f"URL of the file: {dataset_url}")

    df = fetch(dataset_url)
    clean_df = clean(df)
    path = write_local(df, color, dataset_file)

    write_gcs(path)


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str):
    """main ETL flow to load data into Biq Query"""
    path = extract_from_gcs(color, year, month)
    df = read_df(path)
    write_bq(df, color)
    return len(df)


# Main flow here
@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [1, 2, 3], year: int = 2021, color: str = "yellow"
):
    row_start = 0
    for month in months:
        print(f"Doing the ETL for {year}-{month} and {color}")
        etl_web_to_gcs(year, month, color)
        number_of_records = etl_gcs_to_bq(year, month, color)
        row_start += number_of_records

    print(f"number of records of the ingestion: {row_start}")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019

    etl_parent_flow(months, year, color)
