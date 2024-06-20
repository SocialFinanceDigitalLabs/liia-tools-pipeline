from dagster import asset
from decouple import config as env_config
from fs import open_fs
from liiatools.cin_census_pipeline.spec import load_pipeline_config as load_pipeline_config_cin
from liiatools.ssda903_pipeline.spec import load_pipeline_config as load_pipeline_config_ssda903
@asset
def dataset():
    dataset = env_config("DATASET", cast=str)
    return dataset

@asset
def pipeline_config():
    # add argument that will allow either 903 or CIN
    dataset = env_config("DATASET", cast=str)
    if dataset == "CIN":
        return load_pipeline_config_cin()
    elif dataset == "SSDA903":
        return load_pipeline_config_ssda903()
    # leave else statement for errors
    else:
        logger.info("Dataset specified isn't valid. Defaulting to None")
        dataset = None
        return dataset
# add log to check what return load_pipeline_config gives

@asset
def process_folder():
    output_location = env_config("OUTPUT_LOCATION", cast=str)
    return open_fs(output_location)


@asset
def incoming_folder():
    input_location = env_config("INPUT_LOCATION", cast=str)
    return open_fs(input_location)