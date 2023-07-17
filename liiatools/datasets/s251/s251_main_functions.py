from pathlib import Path
import yaml
import logging
import click_log
from datetime import datetime
import numpy as np

# dependencies for cleanfile()
from liiatools.datasets.s251.lds_s251_clean import (
    configuration as clean_config,
    parse,
    populate,
    filters,
    degrade,
    logger,
    file_creator,
    prep,
)

# dependencies for la_agg()
from liiatools.datasets.s251.lds_s251_la_agg import configuration as agg_config
from liiatools.datasets.s251.lds_s251_la_agg import process as agg_process

# dependencies for pan_agg()
from liiatools.datasets.s251.lds_s251_pan_agg import configuration as pan_config
from liiatools.datasets.s251.lds_s251_pan_agg import process as pan_process

from liiatools.datasets.s251.data_generator import sample_data

from liiatools.spec import common as common_asset_dir
from liiatools.datasets.shared_functions.common import (
    flip_dict,
    check_file_type,
    supported_file_types,
    save_incorrect_year_error,
    check_year_within_range,
)

log = logging.getLogger()
click_log.basic_config(log)

COMMON_CONFIG_DIR = Path(common_asset_dir.__file__).parent
# Get all the possible LA codes that could be used
with open(f"{COMMON_CONFIG_DIR}/LA-codes.yml") as las:
    la_list = list(yaml.full_load(las)["data_codes"].values())

YEARS_TO_GO_BACK = 7
YEAR_START_MONTH = 1
REFERENCE_DATE = datetime.now()


def cleanfile(input: str, la_code: str, la_log_dir: str, output: str):
    """
    Cleans input S251 csv file according to config and outputs cleaned csv files.
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param la_log_dir: should specify the path to the local authority's log folder
    :param output: should specify the path to the output folder
    :return: None
    """

    # Prepare and check file
    if prep.check_blank_file(input, la_log_dir=la_log_dir) == "empty":
        return
    prep.drop_empty_rows(input, input)
    if (
        check_file_type(
            input,
            file_types=[".csv"],
            supported_file_types=supported_file_types,
            la_log_dir=la_log_dir,
        )
        == "incorrect file type"
    ):
        return
    year, quarter = prep.find_year_of_return(input, la_log_dir)
    if year is None:
        return

    if (
        check_year_within_range(
            year, YEARS_TO_GO_BACK, YEAR_START_MONTH, REFERENCE_DATE
        )
        is False
    ):
        save_incorrect_year_error(input, la_log_dir)
        return

    # Open & Parse file
    stream = parse.parse_csv(input=input)
    stream = populate.add_year_column(stream, year=year, quarter=quarter)

    # Configure stream
    config = clean_config.Config(year)
    la_name = flip_dict(config["data_codes"])[la_code]
    stream = clean_config.configure_stream(stream, config)

    # Clean stream
    stream = filters.clean(stream)
    stream = degrade.degrade(stream)
    stream = logger.log_errors(stream)
    stream = populate.create_la_child_id(stream, la_code=la_code)

    # Output result
    stream = file_creator.save_stream(stream, la_name=la_name, output=output)
    stream = logger.save_errors_la(stream, la_log_dir=la_log_dir)
    list(stream)


def la_agg(input: str, output: str):
    """
    Joins data from newly cleaned S251 file (output of cleanfile()) to existing S251 data for the depositing local
    authority
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path
    function
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = agg_config.Config()

    # Open file as DataFrame and match file type
    s251_df = agg_process.read_file(input)

    # Merge file with existing file of the same type in LA output folder
    s251_df = agg_process.merge_la_files(output, s251_df)

    # De-duplicate and remove old data according to schema
    dates = config["dates"]
    s251_df = agg_process.convert_datetimes(s251_df, dates)
    sort_order = config["sort_order"]
    dedup = config["dedup"]
    s251_df = agg_process.deduplicate(s251_df, sort_order, dedup)
    s251_df = agg_process.remove_old_data(
        s251_df,
        num_of_years=YEARS_TO_GO_BACK,
        new_year_start_month=YEAR_START_MONTH,
        as_at_date=REFERENCE_DATE,
    )

    # If file still has data, after removing old data: re-format and export merged file
    if len(s251_df) > 0:
        s251_df = agg_process.convert_dates(s251_df, dates)
        agg_process.export_la_file(output, s251_df)


def pan_agg(input: str, la_code: str, output: str):
    """
    Joins data from newly merged S251 file (output of la-agg()) to existing pan-London S251 data
    :param input: should specify the input file location, including file name and suffix, and be usable by a Path function
    :param la_code: should be a three-letter string for the local authority depositing the file
    :param output: should specify the path to the output folder
    :return: None
    """

    # Configuration
    config = pan_config.Config()

    # Read file and match type
    s251_df = pan_process.read_file(input)

    # Remove unwanted datasets and merge wanted with existing output
    la_name = flip_dict(config["data_codes"])[la_code]
    s251_df = pan_process.merge_agg_files(output, s251_df, la_name)
    pan_process.export_pan_file(output, s251_df)


def generate_sample(output: str, la_name: str):
    """
    Export a sample file for testing

    :param output: string containing the desired location and name of sample file
    :param la_name: name of local authority
    :return: .csv sample file in desired location
    """
    stream = sample_data.generate_sample_s251_file()
    stream = file_creator.save_stream(stream, la_name, output=output)
    list(stream)
