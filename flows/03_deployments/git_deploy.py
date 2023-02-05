from prefect.deployments import Deployment
from ETL_web_gcs_bq_homework import etl_parent_flow

from prefect.filesystems import GitHub

storage = GitHub.load("github-prefect-code")

deployment = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-deployment",
    storage=storage,
    entrypoint="flows/02_gcp/ETL_web_gcs_bq_homework.py:etl_parent_flow",
)

if __name__ == "__main__":
    deployment.apply()
