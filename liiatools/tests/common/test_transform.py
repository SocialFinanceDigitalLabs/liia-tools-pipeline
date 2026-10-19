import pandas as pd

from liiatools.common.data import DataContainer, PipelineConfig
from liiatools.common.transform import degrade_data, enrich_data


def _config(input_schema_version=None):
    return PipelineConfig(
        sensor_trigger={},
        retention_columns={},
        retention_period={},
        degrade_at_clean={},
        reports_to_shared={},
        la_signed={},
        input_schema_version=input_schema_version,
        table_list=[
            {
                "id": "placements",
                "columns": [
                    {"id": "child_id", "type": "string"},
                    {
                        "id": "placement_location",
                        "type": "postcode",
                        "degrade": "short_postcode",
                        "exclude": ["GM"],
                    },
                    {
                        "id": "la",
                        "type": "string",
                        "enrich": "la_name",
                        "exclude": ["GM"],
                    },
                ],
            }
        ],
    )


def _data():
    return DataContainer({"placements": pd.DataFrame({"child_id": ["1", "2"]})})


def test_degrade_skips_columns_excluded_for_input_schema_version():
    result = degrade_data(_data(), _config("GM"), {})
    assert len(result.errors) == 0
    assert list(result.data["placements"]["child_id"]) == ["1", "2"]


def test_enrich_skips_columns_excluded_for_input_schema_version():
    result = enrich_data(_data(), _config("GM"), {"la_code": "E08000003"})
    assert len(result.errors) == 0
    assert "la" not in result.data["placements"].columns


def test_degrade_runs_columns_not_excluded_for_input_schema_version():
    data = DataContainer(
        {
            "placements": pd.DataFrame(
                {"child_id": ["1"], "placement_location": ["AB1 2CD"]}
            )
        }
    )
    result = degrade_data(data, _config("NAT"), {})
    assert len(result.errors) == 0
    assert result.data["placements"]["placement_location"].iloc[0] == "AB1 2"
