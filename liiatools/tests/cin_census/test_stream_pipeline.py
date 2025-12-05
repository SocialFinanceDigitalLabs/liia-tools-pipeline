import os
import xml.etree.ElementTree as ET

from fs import open_fs

from liiatools.cin_census_pipeline.spec import load_schema
from liiatools.cin_census_pipeline.spec.samples import CIN_2022
from liiatools.cin_census_pipeline.spec.samples import DIR as SAMPLES_DIR
from liiatools.cin_census_pipeline.stream_pipeline import task_cleanfile
from liiatools.common.data import FileLocator, PipelineConfig

output_config = PipelineConfig(
    sensor_trigger={"move_current_org_sensor": False, "move_concat_sensor": False},
    retention_columns={"year_column": "Year", "la_column": "LA"},
    retention_period={"PAN": 12},
    degrade_at_clean={"PAN": True},
    reports_to_shared={"PAN": True},
    la_signed={
        "Barking and Dagenham": {"PAN": "Yes"},
    },
    table_list=[
        {
            "id": "cin",
            "retain": ["PAN"],
            "columns": [
                {
                    "id": "LAchildID",
                    "type": "string",
                    "unique_key": True,
                    "enrich": ["integer", "add_la_suffix"],
                },
                {"id": "Date", "type": "date", "unique_key": True},
                {"id": "Type", "type": "string", "unique_key": True},
                {"id": "CINreferralDate", "type": "date"},
                {"id": "ReferralSource", "type": "category", "unique_key": True},
                {"id": "PrimaryNeedCode", "type": "category"},
                {"id": "CINclosureDate", "type": "date"},
                {"id": "ReasonForClosure", "type": "category"},
                {"id": "DateOfInitialCPC", "type": "date"},
                {"id": "ReferralNFA", "type": "category"},
                {"id": "CINPlanStartDate", "type": "date"},
                {"id": "CINPlanEndDate", "type": "date"},
                {"id": "S47ActualStartDate", "type": "date"},
                {"id": "InitialCPCtarget", "type": "date", "sort": 2},
                {"id": "ICPCnotRequired", "type": "category"},
                {"id": "AssessmentActualStartDate", "type": "date"},
                {"id": "AssessmentInternalReviewDate", "type": "date", "sort": 1},
                {"id": "AssessmentAuthorisationDate", "type": "date"},
                {"id": "Factors", "type": "category"},
                {"id": "CPPstartDate", "type": "date"},
                {"id": "CPPendDate", "type": "date"},
                {"id": "InitialCategoryOfAbuse", "type": "category"},
                {"id": "LatestCategoryOfAbuse", "type": "category"},
                {"id": "NumberOfPreviousCPP", "type": "numeric"},
                {"id": "UPN", "type": "string"},
                {"id": "FormerUPN", "type": "string"},
                {"id": "UPNunknown", "type": "category"},
                {
                    "id": "PersonBirthDate",
                    "type": "date",
                    "degrade": "first_of_month",
                },
                {
                    "id": "ExpectedPersonBirthDate",
                    "type": "date",
                    "degrade": "first_of_month",
                },
                {"id": "GenderCurrent", "type": "category"},
                {
                    "id": "PersonDeathDate",
                    "type": "date",
                    "degrade": "first_of_month",
                },
                {
                    "id": "PersonSchoolYear",
                    "type": "numeric",
                    "enrich": "school_year",
                },
                {"id": "Ethnicity", "type": "category"},
                {"id": "Disabilities", "type": "category"},
                {"id": "LA", "type": "string", "enrich": "la_name"},
                {"id": "Year", "type": "numeric", "enrich": "year", "sort": 0},
            ],
        }
    ],
)


def test_task_cleanfile():
    samples_fs = open_fs(SAMPLES_DIR.as_posix())
    locator = FileLocator(samples_fs, CIN_2022.name)

    result = task_cleanfile(
        locator, schema=load_schema(2022), output_config=output_config
    )

    data = result.data
    errors = result.errors

    assert len(data) == 1
    assert len(data["cin"]) == 10
    assert len(data["cin"].columns) == 36

    assert len(errors) == 0


def test_task_cleanfile_error():
    tree = ET.parse(CIN_2022)
    root = tree.getroot()

    parent = root.find(".//Source")
    el = parent.find("DateTime")
    el.text = el.text.replace("2022-05-23T11:14:05", "not_date")

    tree.write(SAMPLES_DIR / "cin_2022_error.xml")

    samples_fs = open_fs(SAMPLES_DIR.as_posix())
    locator = FileLocator(samples_fs, "cin_2022_error.xml")

    result = task_cleanfile(
        locator, schema=load_schema(2022), output_config=output_config
    )

    data = result.data
    errors = result.errors

    assert len(data) == 1
    assert len(data["cin"]) == 10
    assert len(data["cin"].columns) == 36

    assert errors[0]["type"] == "ConversionError"
    assert errors[0]["message"] == "Could not convert to date"
    assert errors[0]["filename"] == "cin_2022_error.xml"
    assert errors[0]["header"] == "DateTime"

    os.remove(SAMPLES_DIR / "cin_2022_error.xml")
