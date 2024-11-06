from dagster import get_dagster_logger
import pandas as pd
import numpy as np
import chardet
from fs.base import FS
import fs.errors as errors
from typing import Union, Tuple

log = get_dagster_logger()

# ALL of the code from here to the creation of factOfstedInspection should be ported to the external data pipeline

# Dataset schema for dimension tables
dim_tables = {
    "dimCategoryOfNeed": {
        "CategoryOfNeedKey": [-1, 1, 2, 3, 4, 5, 6, 7, 8],
        "CategoryOfNeedCode": ["-1", "N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"],
        "CategoryOfNeedDescription": [
            "Unknown",
            "Abuse of neglect",
            "Child's disability",
            "Parental illness or disability",
            "Family in acute stress",
            "Family dysfunction",
            "Socially unacceptable behaviour",
            "Low income",
            "Absent parenting",
        ],
    },
    "dimLegalStatus": {
        "LegalStatusKey": [-1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        "LegalStatusCode": [
            "-1",
            "C1",
            "C2",
            "D1",
            "E1",
            "J1",
            "J2",
            "J3",
            "L1",
            "L2",
            "L3",
            "V2",
            "V3",
            "V4",
        ],
        "LegalStatusDescription": [
            "Unknown",
            "Interim care order",
            "Full care order",
            "Freeing order granted",
            "Placement order granted",
            "Remanded to local authority accommodation or to youth detention accommodation",
            "Placed in local authority accommodation under the Police and Criminal Evidence Act 1984, including secure accommodation.  However, this would not necessarily be accommodation where the child would be detained.",
            "Sentenced to Youth Rehabilitation Order (Criminal Justice and Immigration Act 2008 as amended by Legal Aid, Sentencing and Punishment of Offenders Act (LASPOA) 2012 with residence or intensive fostering requirement)",
            "Under police protection and in local authority accommodation",
            "Emergency protection order (EPO)",
            "Under child assessment order and in local authority accommodation",
            "Single period of accommodation under section 20 (Children Act 1989)",
            "Accommodated under an agreed series of short-term breaks, when individual episodes of care are recorded",
            "Accommodated under an agreed series of short-term breaks, when agreements are recorded (NOT individual episodes of care)",
        ],
    },
    "dimOfstedEffectiveness": {
        "OfstedEffectivenessKey": [-1, 1, 2, 3, 4, 5, 6, 7],
        "Grade": [-1, 4, -1, -1, 1, 2, 3, 5],
        "OverallEffectiveness": [
            "Unknown",
            "Adequate",
            "Not provided as service is inspected as part of the ILACS inspection",
            "Not yet inspected",
            "Outstanding",
            "Good",
            "Requires improvement to be good",
            "Inadequate",
        ],
    },
    "dimPlacementProvider": {
        "PlacementProviderKey": [-1, 1, 2, 3, 4, 5, 6],
        "PlacementProviderCode": ["-1", "PR0", "PR1", "PR2", "PR3", "PR4", "PR5"],
        "PlacementProviderDescription": [
            "Unknown",
            "Parent(s) or other person(s) with parental responsibility",
            "Own provision (by the local authority) including a regional adoption agency where the child's responsible local authority is the host authority",
            "Other local authority provision, including a regional adoption agency where another local authority is the host authority",
            "Other public provision (for example, a primary care trust)",
            "Private provision",
            "Voluntary/third sector provision",
        ],
    },
    "dimPlacementType": {
        "PlacementTypeKey": [
            -1,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23,
            24,
            25,
            26,
            27,
        ],
        "PlacementTypeCode": [
            "-1",
            "A3",
            "A4",
            "A5",
            "A6",
            "H5",
            "K1",
            "K2",
            "P1",
            "P2",
            "P3",
            "R1",
            "R2",
            "R3",
            "R5",
            "S1",
            "T0",
            "T1",
            "T2",
            "T3",
            "T4",
            "U1",
            "U2",
            "U3",
            "U4",
            "U5",
            "U6",
            "Z1",
        ],
        "PlacementTypeDescription": [
            "Unknown",
            "Placed for adoption with parental/guardian consent with current foster carer(s) (under Section 19 of the Adoption and Children Act 2002) or with a freeing order where parental/guardian consent has been given (under Section 18(1)(a) of the Adoption Act 1976)",
            "Placed for adoption with parental/guardian consent not with current foster carer(s) (under Section 19 of the Adoption and Children Act 2002) or with a freeing order where parental/guardian consent has been given under Section 18(1)(a) of the Adoption Act 1976",
            "Placed for adoption with placement order with current foster carer(s) (under Section 21 of the Adoption and Children Act 2002) or with a freeing order where parental/guardian consent was dispensed with (under Section 18(1)(b) the Adoption Act 1976)",
            "Placed for adoption with placement order not with current foster carer(s) (under Section 21 of the Adoption and Children Act 2002) or with a freeing order where parental/guardian consent was dispensed with (under Section 18(1)(b) of the Adoption Act 1976)",
            "Semi-independent living accommodation not subject to children's homes regulations",
            "Secure children's homes",
            "Children's Homes subject to Children's Homes Regulations",
            "Placed with own parent(s) or other person(s) with parental responsibility",
            "Independent living for example in a flat, lodgings, bedsit, bed and breakfast (B&B) or with friends, with or without formal support",
            "Residential employment",
            "Residential care home",
            "National Health Service (NHS)/health trust or other establishment providing medical or nursing care",
            "Family centre or mother and baby unit",
            "Young offender institution (YOI)",
            "All residential schools, except where dual-registered as a school and children's home",
            "All types of temporary move (see paragraphs above for further details)",
            "Temporary periods in hospital",
            "Temporary absences of the child on holiday",
            "Temporary accommodation whilst normal foster carer(s) is/are on holiday",
            "Temporary accommodation of seven days or less, for any reason, not covered by codes T1 to T3",
            "Foster placement with relative(s) or friend(s) - long term fostering",
            "Fostering placement with relative(s) or friend(s) who is/are also an approved adopter(s) - fostering for adoption /concurrent planning",
            "Fostering placement with relative(s) or friend(s) who is/are not longterm or fostering for adoption /concurrent planning",
            "Foster placement with other foster carer(s) - long term fostering",
            "Foster placement with other foster carer(s) who is/are also an approved adopter(s) - fostering for adoption /concurrent planning",
            "Foster placement with other foster carer(s) - not long term or fostering for adoption /concurrent planning",
            "Other placements (must be listed on a schedule sent to DfE with annual submission)",
        ],
    },
    "dimReasonEpisodeCeased": {
        "ReasonEpisodeCeasedKey": [
            -1,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
        ],
        "ReasonEpisodeCeasedCode": [
            "-1",
            "E11",
            "E12",
            "E13",
            "E14",
            "E15",
            "E16",
            "E17",
            "E2",
            "E3",
            "E41",
            "E45",
            "E46",
            "E47",
            "E48",
            "E4A",
            "E4B",
            "E5",
            "E6",
            "E7",
            "E8",
            "E9",
            "X1",
        ],
        "ReasonEpisodeCeasedDescription": [
            "Unknown",
            "Adopted - application for an adoption order unopposed",
            "Adopted - consent dispensed with by the court",
            "Left care to live with parent(s), relative(s), or other person(s) with no parental responsibility.",
            "Accommodation on remand ended",
            "Age assessment determined child is aged 18 or over and E5, E6 and E7 do not apply, such as an unaccompanied asylum-seeking child (UASC) whose age has been disputed",
            "Child moved abroad",
            "Aged 18 (or over) and remained with current carers (inc under staying put arrangements)",
            "Died",
            "Care taken over by another local authority in the UK",
            "Residence order (or, from 22 April 2014, a child arrangement order which sets out with whom the child is to live) granted",
            "Special guardianship order made to former foster carer(s), who was/are a relative(s) or friend(s)",
            "Special guardianship order made to former foster carer(s), other than relative(s) or friend(s)",
            "Special guardianship order made to carer(s), other than former foster carer(s), who was/are a relative(s) or friend(s)",
            "Special guardianship order made to carer(s), other than former foster carer(s), other than relative(s) or friend(s)",
            "Returned home to live with parent(s), relative(s), or other person(s) with parental responsibility as part of the care planning process (not under a special guardianship order or residence order or (from 22 April 2014) a child arrangement order).",
            "Returned home to live with parent(s), relative(s), or other person(s) with parental responsibility which was not part of the current care planning process (not under a special guardianship order or residence order or (from 22 April 2014) a child arrangement order).",
            "Moved into independent living arrangement and no longer looked-after: supportive accommodation providing formalised advice/support arrangements (such as most hostels, young men's Christian association, foyers, staying close and care leavers projects). Includes both children leaving care before and at age 18",
            "Moved into independent living arrangement and no longer looked-after : accommodation providing no formalised advice/support arrangements (such as bedsit, own flat, living with friend(s)). Includes both children leaving care before and at age 18",
            "Transferred to residential care funded by adult social care services",
            "Period of being looked-after ceased for any other reason (where none of the other reasons apply)",
            "Sentenced to custody",
            "Episode ceases, and new episode begins on same day, for any reason",
        ],
    },
    "dimReasonForNewEpisode": {
        "ReasonForNewEpisodeKey": [-1, 1, 2, 3, 4, 5, 6],
        "ReasonForNewEpisodeCode": ["-1", "B", "L", "P", "S", "T", "U"],
        "ReasonForNewEpisodeDescription": [
            "Unknown",
            "Change of legal status and placement and carer(s) at the same time",
            "Change of legal status only",
            "Change of placement and carer(s) only",
            "Started to be looked-after",
            "Change of placement (but same carer(s)) only",
            "Change of legal status and change of placement (but same carer(s)) at the same time",
        ],
    },
    "dimReasonPlaceChange": {
        "ReasonPlaceChangeKey": [-1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "ReasonPlaceChangeCode": [
            "-1",
            "ALLEG",
            "APPRR",
            "CARPL",
            "CHILD",
            "CLOSE",
            "CREQB",
            "CREQO",
            "CUSTOD",
            "LAREQ",
            "OTHER",
            "PLACE",
            "STAND",
        ],
        "ReasonPlaceChangeDescription": [
            "Unknown",
            "Allegation (s47)",
            "Approval removed",
            "Change to/Implementation of Care Plan",
            "Child requests placement end",
            "Resignation/ closure of provision",
            "Carer(s) requests placement end due to child's behaviour",
            "Carer(s) requests placement end other than due to child's behaviour",
            "Custody arrangement",
            "Responsible/area authority requests placement end",
            "Other",
            "Change in the status of placement only",
            "Standards of care concern",
        ],
    },
}

# Dictionary contain column name and data type mapping for data tables
column_names = {
    "ONSArea": {
        "name_map": {
            "_WD21CD": "WardCode",
            "WD21NM": "WardName",
            "LAD21CD": "LACode",
            "LAD21NM": "LAName",
            "CTY21CD": "CountyCode",
            "CTY21NM": "CountyName",
            "RGN21CD": "RegionCode",
            "RGN21NM": "RegionName",
            "CTRY21CD": "CountryCode",
            "CTRY21NM": "CountryName",
        },
        "type_map": {
            "WardCode": "cat",
            "WardName": "cat",
            "LACode": "cat",
            "LAName": "cat",
            "CountyCode": "cat",
            "CountyName": "cat",
            "RegionCode": "cat",
            "RegionName": "cat",
            "CountryCode": "cat",
            "CountryName": "cat",
            "AreaType": "cat",
            "AreaCode": "cat",
            "AreaName": "cat",
            "ONSAreaKey": "key",
        },
    },
    "Postcode": {
        "name_map": {
            "pcd2": "Sector",
            "oslaua": "ONSAreaCode",
            "lsoa11": "LSOA2011",
            "lat": "Latitude",
            "long": "Longitude",
            "oseast1m": "OSEastings",
            "osnrth1m": "OSNorthings",
            "imd": "IMD",
        },
        "type_map": {
            "Sector": "cat",
            "ONSAreaCode": "cat",
            "LSOA2011": "cat",
            "Latitude": "num",
            "Longitude": "num",
            "OSEastings": "num",
            "OSNorthings": "num",
            "IMD": "num",
            "PostcodeKey": "key",
        },
    },
    "OfstedProvider": {
        "name_map": {
            "URN": "URN",
            "Provision type": "ProviderType",
            "Sector": "Sector",
            "Registration date": "RegistrationDate",
            "Registration status": "ProviderStatus",
            "Date closed": "ClosedDate",
            "Places": "MaxUsers",
            "UnknownSourceFlag": "UnknownSourceFlag",
            "Organisation which owns the provider": "OwnerName",
            "AreaCode": "ONSAreaCode",
        },
        "type_map": {
            "URN": "cat",
            "ProviderType": "cat",
            "Sector": "cat",
            "RegistrationDate": "date",
            "ProviderStatus": "cat",
            "ClosedDate": "date",
            "MaxUsers": "num",
            "UnknownSourceFlag": "bool",
            "OwnerName": "cat",
            "ONSAreaCode": "cat",
            "OfstedProviderKey": "key",
        },
    },
    "OfstedInspection": {
        "name_map": {
            "OfstedProviderKey": "OfstedProviderKey",
            "Inspection date": "InspectionDateKey",
            "EndDateKey": "EndDateKey",
            "OfstedEffectivenessKey": "OfstedEffectivenessKey",
            "IsLatest": "IsLatest",
        },
        "type_map": {
            "OfstedProviderKey": "key",
            "InspectionDateKey": "date",
            "EndDateKey": "date",
            "OfstedEffectivenessKey": "key",
            "IsLatest": "bool",
            "factOfstedInspectionKey": "key",
        },
    },
    "LookedAfterChild": {
        "name_map": {
            "CHILD": "ChildIdentifier",
            "SEX": "Gender",
            "DOB": "DateofBirthKey",
            "ETHNIC": "EthnicCode",
            "DUC": "UASCCeasedDateKey",
            "YEAR": "SubmissionYearDateKey",
            "ONSAreaKey": "ONSAreaCode",
        },
        "type_map": {
            "ChildIdentifier": "cat",
            "Gender": "cat",
            "DateofBirthKey": "date",
            "EthnicCode": "cat",
            "UASCCeasedDateKey": "date",
            "SubmissionYearDateKey": "cat",
            "ONSAreaCode": "key",
            "LookedAfterChildKey": "key",
        },
    },
    "Episode": {
        "name_map": {
            "LookedAfterChildKey": "LookedAfterChildKey",
            "DECOM": "EpisodeCommencedDateKey",
            "ReasonForNewEpisodeKey": "ReasonForNewEpisodeKey",
            "LegalStatusKey": "LegalStatusKey",
            "CategoryOfNeedKey": "CategoryOfNeedKey",
            "PlacementTypeKey": "PlacementTypeKey",
            "PlacementProviderKey": "PlacementProviderKey",
            "DEC": "EpisodeCeasedDateKey",
            "ReasonEpisodeCeasedKey": "ReasonEpisodeCeasedKey",
            "ReasonPlaceChangeKey": "ReasonPlaceChangeKey",
            "HomePostcodeKey": "HomePostcodeKey",
            "PlacementPostcodeKey": "PlacementPostcodeKey",
            "OfstedProviderKey": "OfstedProviderKey",
            "ONSAreaKey": "ONSAreaKey",
            "YEAR": "SubmissionYearDateKey",
        },
        "type_map": {
            "LookedAfterChildKey": "key",
            "EpisodeCommencedDateKey": "date",
            "ReasonForNewEpisodeKey": "key",
            "LegalStatusKey": "key",
            "CategoryOfNeedKey": "key",
            "PlacementTypeKey": "key",
            "PlacementProviderKey": "key",
            "EpisodeCeasedDateKey": "date",
            "ReasonEpisodeCeasedKey": "key",
            "ReasonPlaceChangeKey": "key",
            "HomePostcodeKey": "key",
            "PlacementPostcodeKey": "key",
            "OfstedProviderKey": "key",
            "ONSAreaKey": "key",
            "SubmissionYearDateKey": "cat",
            "FactEpisodeKey": "key",
        },
    },
}

replace_values = {
    "Local authority": {
        "Westmorland and Furness": "Westmoreland",
        "Bristol": "Bristol, City of",
        "Bournemouth, Christchurch & Poole": "Bournemouth, Christchurch and Poole",
        "Durham": "County Durham",
        "Cumberland": "Carlisle",
        "Herefordshire": "Herefordshire, County of",
        "Kingston upon Hull": "Kingston upon Hull, City of",
        "Southend on Sea": "Southend-on-Sea",
    },
    "EthnicDescription": {
        "WBRI": "White British",
        "WIRI": "White Irish",
        "WOTH": "Any other White background",
        "WIRT": "Traveller of Irish Heritage",
        "WROM": "Gypsy/Roma",
        "MWBC": "White and Black Caribbean",
        "MWBA": "White and Black African",
        "MWAS": "White and Asian",
        "MOTH": "Any other Mixed background",
        "AIND": "Indian",
        "APKN": "Pakistani",
        "ABAN": "Bangladeshi",
        "CHNE": "Chinese",
        "AOTH": "Any other Asian background",
        "BCRB": "Caribbean",
        "BAFR": "African",
        "BOTH": "Any other Black background",
        "OOTH": "Any other ethnic group",
        "REFU": "Refused",
        "NOBT": "Information not yet obtained",
        "Unknown": "Unknown",
    },
    "UASCStatusDescription": {
        0: "Child was not an unaccompanied asylum-seeking child (UASC) at any time during the year",
        1: "Child was an unaccompanied asylum-seeking child (UASC) during the year",
    },
    "Gender": {1.0: "Male", 1: "Male", 2.0: "Female", 2: "Female", -1: "Unknown"},
}


def dict_to_dfs() -> dict:
    """Turns a dictionary of dictionaries into a dictionary of pandas DataFrames"""
    dim_dfs = {}
    for table_name, table_data in dim_tables.items():
        dim_dfs[table_name] = pd.DataFrame(table_data)
    return dim_dfs


def open_file(fs: FS, file: str) -> pd.DataFrame:
    """
    Opens a file within a pyfilesystem
    """
    # Check file encoding
    encoding = check_encoding(fs, file)
    # Open the CSV file using the FS URL
    with fs.open(file, "rb") as f:
        # Read the file content into a pandas DataFrame
        df = pd.read_csv(f, encoding=encoding)
    df = drop_blank_columns(df)
    return df


def check_encoding(fs: FS, file_path: str) -> str:
    """
    Check encoding of a file
    """
    file = fs.open(file_path, "rb")

    bytes_data = file.read()  # Read as bytes
    result = chardet.detect(bytes_data)  # Detect encoding on bytes
    return result["encoding"]


def drop_blank_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Removes columns with no names in a pandas DataFrame"""
    unnamed_columns = [col for col in df.columns if "Unnamed" in col]
    df = df.drop(columns=unnamed_columns)
    return df


def ons_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Performs steps to transform ONSArea table"""
    # Rename columns and drop unnecessary columns
    df = rename_and_drop(df, "ONSArea")

    # Create AreaType, AreaCode and AreaName fields to allow a single primary key to access all area types
    # For wards
    ward_df = df.copy()
    ward_df["AreaType"] = "Ward"
    ward_df["AreaCode"] = ward_df["WardCode"]
    ward_df["AreaName"] = ward_df["WardName"]

    # For LAs
    la_df = df.copy()[
        [
            "LACode",
            "LAName",
            "CountyCode",
            "CountyName",
            "RegionCode",
            "RegionName",
            "CountryCode",
            "CountryName",
        ]
    ]
    mask_la = la_df["LACode"].notna()
    la_df.loc[mask_la, "AreaType"] = "LA"
    la_df.loc[mask_la, "AreaCode"] = la_df["LACode"]
    la_df.loc[mask_la, "AreaName"] = la_df["LAName"]

    # For counties
    county_df = df.copy()[
        [
            "CountyCode",
            "CountyName",
            "RegionCode",
            "RegionName",
            "CountryCode",
            "CountryName",
        ]
    ]
    mask_county = county_df["CountyCode"].notna()
    county_df.loc[mask_county, "AreaType"] = "County"
    county_df.loc[mask_county, "AreaCode"] = county_df["CountyCode"]
    county_df.loc[mask_county, "AreaName"] = county_df["CountyName"]

    # For regions
    region_df = df.copy()[
        [
            "RegionCode",
            "RegionName",
            "CountryCode",
            "CountryName",
        ]
    ]
    mask_region = region_df["RegionCode"].notna()
    region_df.loc[mask_region, "AreaType"] = "Region"
    region_df.loc[mask_region, "AreaCode"] = region_df["RegionCode"]
    region_df.loc[mask_region, "AreaName"] = region_df["RegionName"]

    # For countries
    country_df = df.copy()[
        [
            "CountryCode",
            "CountryName",
        ]
    ]
    mask_country = country_df["CountryCode"].notna()
    country_df.loc[mask_country, "AreaType"] = "Country"
    country_df.loc[mask_country, "AreaCode"] = country_df["CountryCode"]
    country_df.loc[mask_country, "AreaName"] = country_df["CountryName"]

    # Joining together into a single file and dropping duplicates and rows with no AreaType defined
    expanded_df = pd.concat([ward_df, la_df, county_df, region_df, country_df]).drop_duplicates()
    expanded_df.dropna(subset="AreaType", inplace=True)

    # Reset indexes and use main index as primary key
    expanded_df.reset_index(drop=True, inplace=True)
    expanded_df.reset_index(inplace=True, names="ONSAreaKey")

    # Add a row to return when a foreign key returns no match
    expanded_df = add_nan_row(expanded_df)

    # Replace missing values
    expanded_df = fill_missing_values(expanded_df, "ONSArea")

    return expanded_df


def postcode_transform(df: pd.DataFrame) -> pd.DataFrame:
    """Performs steps to transform Postcode table"""
    # Rename columns and drop unnecessary columns
    df = rename_and_drop(df, "Postcode")

    # Reset indexes and use main index as primary key
    df.reset_index(drop=True, inplace=True)
    df.reset_index(inplace=True, names="PostcodeKey")

    # Add a row to return when a foreign key returns no match
    df = add_nan_row(df)

    # Replace missing values
    df = fill_missing_values(df, "Postcode")

    return df


def ofsted_transform(fs: FS, ONSArea: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Creates a dimension table with all Ofsted providers and a fact table with all Ofsted inspections"""
    try:
        fs.makedir("Ofsted")
    except errors.DirectoryExists:
        pass
    fs_ofs = fs.opendir("Ofsted")

    # Find the number of years' data present in the folder
    directory_contents = fs_ofs.listdir("")
    year_list = [f[-6:-4] for f in directory_contents]
    unique_years = set(year_list)

    # Ensure that there is at least one year of data
    if len(unique_years) == 0:
        log.error("No Ofsted data present in external data folder")
        return None, None

    # Create OfstedProvider table
    # For each year, create a unique record for each provider and put in dictionary
    # Each year must have a copy of three files to produce a useful output
    # If no years meet this standard, table cannot be produced and process is terminated
    provider_df_dict = {}
    for year in unique_years:
        last_year = int(year) - 1

        # Open providers_in_year file
        providers_in_file = f"Provider_level_in_year_20{last_year}-{year}.csv"
        try:
            providers_in_df = open_file(fs_ofs, providers_in_file)
        except errors.ResourceNotFound:
            log.info(f"No provider in year file for {year}")
            providers_in_df = None

        # Open closed file
        closed_file = f"Closed_childrens_homes_31Mar{year}.csv"
        try:
            closed_df = open_file(fs_ofs, closed_file)
        except errors.ResourceNotFound:
            log.info(f"No closed provider file for {year}")
            closed_df = None

        # Open providers_places file
        providers_places_file = f"Providers_places_at_31_Aug_20{year}.csv"
        try:
            providers_places = open_file(fs_ofs, providers_places_file)
            providers_places = providers_places.rename(
                columns={"Organisation name": "Organisation which owns the provider"}
            )
        except errors.ResourceNotFound:
            log.info(f"No providers places file for 20{year}")
            providers_places = None

        # If all three files exist for the year, create an output
        if providers_in_df is not None and closed_df is not None and providers_places is not None:
            # Merge providers_in and providesr_closed on URN to add closure info to richer providers_in record
            providers_closed = providers_in_df.merge(closed_df, on="URN", how="outer")

            # Concatenate providers_places with merged providers_closed df
            providers_df = pd.concat([providers_places, providers_closed])
            providers_df = providers_df.drop_duplicates(subset="URN", keep="first")

            # Add year to table and output to dictionary if output created
            providers_df["Year"] = year
            provider_df_dict[year] = providers_df

    # Check that at least one year has provided an output
    # If not, terminate process as not enough data to create table
    if len(provider_df_dict) == 0:
        log.error("Insufficient Ofsted data to create dimOfstedProvider table")
        return None, None
    # Across the years in the dictionary, concatenate to single file and drop duplicates
    OfstedProvider = concat_and_drop(provider_df_dict, "Year", "URN")

    # Add bespoke columns
    # Add UnknownSourceFlag with False for existing rows
    OfstedProvider["UnknownSourceFlag"] = False

    # Add ONSAreaKey by merge with ONSArea table
    OfstedProvider["Local authority"] = OfstedProvider["Local authority"].replace(
        replace_values["Local authority"]
    )

    OfstedProvider = OfstedProvider.merge(
        ONSArea.copy(), left_on="Local authority", right_on="AreaName", how="left"
    )

    # Rename columns and drop unnecessary columns
    OfstedProvider = rename_and_drop(OfstedProvider, "OfstedProvider")

    # Reset indexes and use main index as primary key
    OfstedProvider.reset_index(drop=True, inplace=True)
    OfstedProvider.reset_index(inplace=True, names="OfstedProviderKey")

    # Add nan rows: one for unmatched URNs, one for missing URNs and one for XXXXXXX (regional adoption agencies)
    unmatched_row = pd.DataFrame([[np.nan] * len(OfstedProvider.columns)], columns=OfstedProvider.columns)
    unmatched_row["OfstedProviderKey"] = -3
    unmatched_row["UnknownSourceFlag"] = True
    OfstedProvider = pd.concat([OfstedProvider, unmatched_row], ignore_index=True)

    raa_row = pd.DataFrame([[np.nan] * len(OfstedProvider.columns)], columns=OfstedProvider.columns)
    raa_row["OfstedProviderKey"] = -2
    raa_row["URN"] = "XXXXXXX"
    raa_row["ProviderType"] = "Regional Adoption Agency"
    raa_row["UnknownSourceFlag"] = True
    OfstedProvider = pd.concat([OfstedProvider, raa_row], ignore_index=True)

    missing_row = pd.DataFrame([[np.nan] * len(OfstedProvider.columns)], columns=OfstedProvider.columns)
    missing_row["OfstedProviderKey"] = -1
    missing_row["URN"] = "Missing"
    missing_row["UnknownSourceFlag"] = True
    OfstedProvider = pd.concat([OfstedProvider, missing_row], ignore_index=True)

    # Replace missing values
    OfstedProvider = fill_missing_values(OfstedProvider, "OfstedProvider")

    # Create OfstedInspection table
    # For each year, create a unique record for each provider and put in dictionary
    # Either the provider_at or the provider_in file is sufficient to create a record each year; if both exist, concat to create single file
    inspection_df_dict = {}
    for year in unique_years:
        last_year = int(year) - 1

        # Open provider at file
        provider_at_file = f"Provider_level_at_31_Aug_20{year}.csv"
        try:
            provider_at_df = open_file(fs_ofs, provider_at_file)
            provider_at_df = provider_at_df.rename(
                columns={"Latest full inspection date": "Inspection date"}
            )
        except errors.ResourceNotFound:
            log.info(f"No provider at file for {year}")
            provider_at_df = None

        # Open providers_in_year file and keep only full inspections
        provider_in_file = f"Provider_level_in_year_20{last_year}-{year}.csv"
        try:
            provider_in_df = open_file(fs_ofs, provider_in_file)
            provider_in_df = provider_in_df.loc[
                provider_in_df["Inspection event type"] == "Full inspection"
            ]
        except errors.ResourceNotFound:
            log.info(f"No provider in file for {year}")
            provider_in_df = None

        if provider_at_df is not None and provider_in_df is not None:
            # Concatenate provider_at with provider_in
            inspections_df = pd.concat([provider_at_df, provider_in_df])
            inspections_df = inspections_df.drop_duplicates(
                subset=["URN", "Inspection date"], keep="first"
            )
        elif provider_at_df is not None:
            inspections_df = provider_at_df
        elif provider_in_df is not None:
            inspections_df = provider_in_df
        else:
            inspections_df = None

        if inspections_df is not None:
            # Add year to table and output to dictionary
            inspections_df["Year"] = year
            inspection_df_dict[year] = inspections_df

    # If there was insufficient data for a single year, terminate the process
    if len(inspection_df_dict) == 0:
        log.error("Insufficient data to create factOfstedInspection table")
        return None, None
    # Across the years in the dictionary, concatenate to single file and drop duplicates
    factOfstedInspection = concat_and_drop(
        inspection_df_dict, "Year", ["URN", "Inspection date"]
    )

    # Add bespoke columns
    # Add OfstedProviderKey by merge with OfstedProvider table
    factOfstedInspection = factOfstedInspection.merge(
        OfstedProvider.copy(), how="left", on="URN"
    )

    # Add OfstedEffectivenessKey by merge with dimOfstedEffectiveness
    ofsted_effectiveness = pd.DataFrame(dim_tables["dimOfstedEffectiveness"])
    factOfstedInspection = factOfstedInspection.merge(
        ofsted_effectiveness,
        how="left",
        left_on="Overall experiences and progress of children and young people",
        right_on="OverallEffectiveness",
    )

    # Add IsLatest column that indicates if inspection is the most recent one recorded for a URN
    factOfstedInspection["IsLatest"] = (
        factOfstedInspection.groupby("OfstedProviderKey")["Inspection date"].transform(
            "max"
        )
        == factOfstedInspection["Inspection date"]
    )

    # Add EndDateKey column that denotes the end date that an inspection applies to
    # Sort to get providers and inspection dates in order
    factOfstedInspection.sort_values(
        by=["OfstedProviderKey", "Inspection date"],
        inplace=True,
    )
    # where isLatest is False, EndDateKey should be the next chronological inspection date for that URN
    factOfstedInspection.loc[
        ~factOfstedInspection["IsLatest"], "EndDateKey"
    ] = factOfstedInspection["Inspection date"].shift(-1)

    # Rename columns and drop unnecessary columns
    factOfstedInspection = rename_and_drop(factOfstedInspection, "OfstedInspection")

    # Reset indexes and use main index as primary key
    factOfstedInspection.reset_index(drop=True, inplace=True)
    factOfstedInspection.reset_index(inplace=True, names="factOfstedInspectionKey")

    # Replace missing values
    factOfstedInspection = fill_missing_values(factOfstedInspection, "OfstedInspection")

    return OfstedProvider, factOfstedInspection


def ss903_transform(
    header: pd.DataFrame,
    uasc: pd.DataFrame,
    ONSArea: pd.DataFrame,
    Episode: pd.DataFrame,
    Postcode: pd.DataFrame,
    OfstedProvider: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Takes three ssda903 files (header, episodes and uasc) and creates:
    - a dimension table with one row per child
    - a fact table with one row per episode"""
    # Create LookedAfterChild table
    # Drop header rows with no primary key
    header.dropna(subset="CHILD", inplace=True)

    # Merge with subset of UASC
    uasc = uasc[["CHILD", "DUC", "YEAR"]]
    LookedAfterChild = header.merge(uasc, on=["CHILD", "YEAR"], how="left")

    # Add ONSAreaKey by merge with ONSArea table
    LookedAfterChild = LookedAfterChild.merge(
        ONSArea, left_on="LA", right_on="AreaName", how="left"
    )

    # Rename columns and drop unnecessary columns
    LookedAfterChild = rename_and_drop(LookedAfterChild, "LookedAfterChild")

    # Reset indexes and use main index as primary key
    LookedAfterChild.reset_index(drop=True, inplace=True)
    LookedAfterChild.reset_index(inplace=True, names="LookedAfterChildKey")

    # Add a row to return when a foreign key returns no match
    LookedAfterChild = add_nan_row(LookedAfterChild)

    # Replace missing values
    LookedAfterChild = fill_missing_values(LookedAfterChild, "LookedAfterChild")

    # Add bespoke columns
    # Add EthnicDescription from EthnicCode lookup
    LookedAfterChild["EthnicDescription"] = LookedAfterChild.EthnicCode.map(
        replace_values["EthnicDescription"]
    )

    # Generate additional UASC fields using UASCCeasedDDateKey
    LookedAfterChild["UASCStatusCode"] = LookedAfterChild.UASCCeasedDateKey.map(
        lambda x: 0 if x == "2999-12-31" else 1
    )
    LookedAfterChild["UASCStatusDescription"] = LookedAfterChild.UASCStatusCode.map(
        replace_values["UASCStatusDescription"]
    )

    # Replace numerical values in Gender with strings
    LookedAfterChild["Gender"] = LookedAfterChild["Gender"].replace(
        replace_values["Gender"]
    )

    # Create factEpisode table
    # Add bespoke columns
    # Add LookedAfterChildKey by merge with LookedAfterChild table
    lac_copy = LookedAfterChild.copy()
    lac_copy = lac_copy[["LookedAfterChildKey", "ChildIdentifier"]]
    Episode = Episode.merge(
        lac_copy, left_on="CHILD", right_on="ChildIdentifier", how="left"
    )

    # Add ReasonForNewEpisodeKey
    reason_for_new_episode = pd.DataFrame(dim_tables["dimReasonForNewEpisode"])
    reason_for_new_episode = reason_for_new_episode[
        ["ReasonForNewEpisodeKey", "ReasonForNewEpisodeCode"]
    ]
    Episode = Episode.merge(
        reason_for_new_episode,
        left_on="RNE",
        right_on="ReasonForNewEpisodeCode",
        how="left",
    )

    # Add LegalStatusKey
    legalstatuskey = pd.DataFrame(dim_tables["dimLegalStatus"])
    legalstatuskey = legalstatuskey[["LegalStatusKey", "LegalStatusCode"]]
    Episode = Episode.merge(
        legalstatuskey, left_on="LS", right_on="LegalStatusCode", how="left"
    )

    # Add CategoryOfNeedKey
    category_of_need = pd.DataFrame(dim_tables["dimCategoryOfNeed"])
    category_of_need = category_of_need[["CategoryOfNeedKey", "CategoryOfNeedCode"]]
    Episode = Episode.merge(
        category_of_need, left_on="CIN", right_on="CategoryOfNeedCode", how="left"
    )

    # Add PlacementTypeKey
    placement_type = pd.DataFrame(dim_tables["dimPlacementType"])
    placement_type = placement_type[["PlacementTypeKey", "PlacementTypeCode"]]
    Episode = Episode.merge(
        placement_type, left_on="PLACE", right_on="PlacementTypeCode", how="left"
    )

    # Add PlacementProviderKey
    placement_provider = pd.DataFrame(dim_tables["dimPlacementProvider"])
    placement_provider = placement_provider[
        ["PlacementProviderKey", "PlacementProviderCode"]
    ]
    Episode = Episode.merge(
        placement_provider,
        left_on="PLACE_PROVIDER",
        right_on="PlacementProviderCode",
        how="left",
    )

    # Add ReasonEpisodeCeasedKey
    reason_episode_ceased = pd.DataFrame(dim_tables["dimReasonEpisodeCeased"])
    reason_episode_ceased = reason_episode_ceased[
        ["ReasonEpisodeCeasedKey", "ReasonEpisodeCeasedCode"]
    ]
    Episode = Episode.merge(
        reason_episode_ceased,
        left_on="REC",
        right_on="ReasonEpisodeCeasedCode",
        how="left",
    )

    # Add ReasonPlaceChangeKey
    reason_place_change = pd.DataFrame(dim_tables["dimReasonPlaceChange"])
    reason_place_change = reason_place_change[
        ["ReasonPlaceChangeKey", "ReasonPlaceChangeCode"]
    ]
    Episode = Episode.merge(
        reason_place_change,
        left_on="REASON_PLACE_CHANGE",
        right_on="ReasonPlaceChangeCode",
        how="left",
    )

    # Add HomePostcodeKey
    pc_copy = Postcode.copy()[["PostcodeKey", "Sector"]]
    Episode = Episode.merge(
        pc_copy, left_on="HOME_POST", right_on="Sector", how="left"
    )
    Episode = Episode.rename(columns={"PostcodeKey": "HomePostcodeKey"})

    # Add PlacementPostcodeKey
    Episode = Episode.merge(pc_copy, left_on="PL_POST", right_on="Sector", how="left")
    Episode = Episode.rename(columns={"PostcodeKey": "PlacementPostcodeKey"})

    # Add OfstedProviderKey
    op_copy = OfstedProvider.copy()[["OfstedProviderKey", "URN"]]
    op_copy.URN = op_copy.URN.astype(str)
    Episode.URN = Episode.URN.astype(str)
    Episode = Episode.merge(op_copy, left_on="URN", right_on="URN", how="left")
    # Where no match with OfstedProvider, give a value of -3 to differentiate from missing URNs
    Episode.loc[
        (Episode["URN"].notna()) & (Episode["OfstedProviderKey"].isna()),
        "OfstedProviderKey",
    ] = -3

    # Add ONSAreaKey
    ons_copy = ONSArea.copy()[["ONSAreaKey", "AreaName"]]
    Episode = Episode.merge(ons_copy, left_on="LA", right_on="AreaName", how="left")

    # Rename columns and drop unnecessary columns
    Episode = rename_and_drop(Episode, "Episode")

    # Reset indexes and use main index as primary key
    Episode.reset_index(drop=True, inplace=True)
    Episode.reset_index(inplace=True, names="FactEpisodeKey")

    # Replace missing values
    Episode = fill_missing_values(Episode, "Episode")

    return LookedAfterChild, Episode


def rename_and_drop(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Takes a pandas DataFrame and:
    - renames columns based on dictionary
    - drops unnecessary columns based on dictionary"""
    df.rename(columns=column_names[key]["name_map"], inplace=True)
    df = df[list(column_names[key]["name_map"].values())]
    return df


def add_nan_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a lookup for other tables with missing key values
    """
    nan_row = pd.DataFrame([[np.nan] * len(df.columns)], columns=df.columns)
    df = pd.concat([df, nan_row], ignore_index=True)
    return df


def fill_missing_values(df: pd.DataFrame, df_name: str) -> pd.DataFrame:
    """Checks for missing values in the df and replaces based on type, where type is derived from dictionary"""
    types = column_names[df_name]["type_map"]

    for col, col_type in types.items():
        if col_type == "cat":
            df[col] = df[col].fillna("Unknown")
        elif col_type == "key":
            df[col] = df[col].fillna(-1)
        elif col_type == "date":
            df[col] = df[col].fillna("2999-12-31")
        elif col_type == "num":
            df[col] = df[col].fillna(np.nan).replace([np.nan], [None])
        elif col_type == "bool":
            df[col] = df[col].fillna(np.nan)

    return df


def concat_and_drop(dict: dict, sortby: str, dropset: Union[str, list]) -> pd.DataFrame:
    df = pd.concat(dict)
    df.sort_values(by=sortby, ascending=False)
    df.drop_duplicates(subset=dropset, inplace=True)

    return df
