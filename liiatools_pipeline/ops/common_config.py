from dagster import Config


class CleanConfig(Config):
    dataset_folder: str | None
    la_folder: str | None
    input_la_code: str | None
    dataset: str | None
