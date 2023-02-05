from prefect import flow
from prefect.blocks.system import Secret


@flow
def add_secret_block():
    Secret(value="passwordexample").save(name="jonassecret")


if __name__ == "__main__":
    add_secret_block()
