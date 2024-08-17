from dagster import op

from liiatools.common.data import DataContainer
from liiatools.ssda903_pipeline.sufficiency_transform import (
    dict_to_dfs,
    open_file,
    ons_transform,
    postcode_transform,
    ofsted_transform,
    ss903_transform
)
from liiatools_pipeline.assets.common import (
    shared_folder,
    workspace_folder
)
from liiatools_pipeline.assets.external_dataset import (
    external_data_folder
)

# This op should be ported to the external_dataset pipeline as it only needs to run once
@op
def output_lookup_tables():
    output_folder = shared_folder()
    dim_dfs = dict_to_dfs()
    dim_dfs = DataContainer(dim_dfs)
    dim_dfs.export(output_folder, "", "csv")

# This op should be ported to the external_dataset pipeine as it only needs to run when external inputs are changed
@op
def create_dim_fact_tables():
    ext_folder = external_data_folder()
    workspace = workspace_folder()
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
    s903_folder = workspace.opendir("current/ssda903/PAN")
    LookedAfterChild = open_file(s903_folder, "ssda903_Header.csv")
    UASC = open_file(s903_folder, "ssda903_UASC.csv")
    Episode = open_file(s903_folder, "ssda903_Episodes.csv")

    # Transform tables
    LookedAfterChild, factEpisode = ss903_transform(LookedAfterChild, UASC, ONSArea, Episode, Postcode, OfstedProvider)
    dim_tables["dimLookedAfterChild"] = LookedAfterChild
    dim_tables["factEpisode"] = factEpisode

    # Export tables
    dim_tables = DataContainer(dim_tables)
    dim_tables.export(output_folder, "", "csv")
