from dagster import asset
from decouple import config as env_config
from fs import open_fs
from liiatools.cin_census_pipeline.spec import load_pipeline_config


@asset
def pipeline_config_cin():
    return load_pipeline_config()


@asset
def process_folder_cin():
    output_location = env_config("OUTPUT_LOCATION", cast=str)
    return open_fs(output_location)


@asset
def incoming_folder_cin():
    input_location = env_config("INPUT_LOCATION", cast=str)
    return open_fs(input_location)
