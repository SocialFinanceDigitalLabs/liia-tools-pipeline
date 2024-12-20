import re

from dagster import In, Out, op
from fs.base import FS

from liiatools.common import pipeline as pl
from liiatools.common.constants import SessionNamesSufficiency
from liiatools.common.data import DataContainer
from liiatools.ssda903_pipeline.sufficiency_transform import (
    dict_to_dfs, ofsted_transform, ons_transform, open_file,
    postcode_transform, ss903_transform)
from liiatools_pipeline.assets.common import shared_folder, workspace_folder
from liiatools_pipeline.assets.external_dataset import external_data_folder


# This op should be ported to the external_dataset pipeline as it only needs to run once
@op
def output_lookup_tables():
    dim_dfs = dict_to_dfs()
    dim_dfs = DataContainer(dim_dfs)
    dim_dfs.export(shared_folder(), "", "csv")


@op(
    out={
        "session_folder": Out(FS),
    }
)
def create_sufficiency_session_folder() -> FS:
    session_folder, session_id = pl.create_session_folder(
        workspace_folder(), SessionNamesSufficiency
    )
    session_folder = session_folder.opendir(SessionNamesSufficiency.INCOMING_FOLDER)

    reports_folder = workspace_folder().opendir("current/ssda903/SUFFICIENCY")
    pl.move_files_for_sharing(
        reports_folder, session_folder, required_table_id=["episodes", "header", "uasc"]
    )

    return session_folder


@op(
    ins={
        "session_folder": In(FS),
    },
)
def create_dim_fact_tables(
    session_folder: FS,
):
    # Check that the files necessary for the job are in the folder
    episodes = re.compile(r"episodes")
    header = re.compile(r"header")
    uasc = re.compile(r"uasc")
    pattern_list = [episodes, header, uasc]

    files = session_folder.listdir("/")

    all_files_present = all(
        any(pattern.search(filename) for filename in files) for pattern in pattern_list
    )

    # Run the data transformation if all necessary files are present
    if all_files_present:
        ext_folder = external_data_folder()
        output_folder = shared_folder()

        # Create empty dictionary to store tables
        dim_tables = {}

        # Create dimONSArea table
        # Open external file
        ONSArea = open_file(ext_folder, "ONS_Area.csv")
        # Transform ONSArea table
        ONSArea = ons_transform(ONSArea)
        dim_tables["dimONSArea"] = ONSArea

        # Create dimPostcode table
        # Open external file
        Postcode = open_file(ext_folder, "ONSPD_reduced_to_postcode_sector.csv")
        # Transform Postcode table
        Postcode = postcode_transform(Postcode)
        dim_tables["dimPostcode"] = Postcode

        # Create dimOfstedProvider and factOfstedInspection tables
        # Open and transform files
        OfstedProvider, factOfstedInspection = ofsted_transform(ext_folder, ONSArea)
        dim_tables["dimOfstedProvider"] = OfstedProvider
        dim_tables["factOfstedInspection"] = factOfstedInspection

        # Create dimLookedAfterChild and factEpisode table
        # Open ssda903 files
        LookedAfterChild = open_file(session_folder, "ssda903_header.csv")
        UASC = open_file(session_folder, "ssda903_uasc.csv")
        Episode = open_file(session_folder, "ssda903_episodes.csv")

        # Transform tables
        LookedAfterChild, factEpisode = ss903_transform(
            LookedAfterChild, UASC, ONSArea, Episode, Postcode, OfstedProvider
        )
        dim_tables["dimLookedAfterChild"] = LookedAfterChild
        dim_tables["factEpisode"] = factEpisode

        # Export tables
        dim_tables = DataContainer(dim_tables)
        dim_tables.export(output_folder, "", "csv")
