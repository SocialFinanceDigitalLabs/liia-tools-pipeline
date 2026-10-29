from datetime import date

import numpy as np
import pandas as pd
import pytest

from liiatools.placements_standard_pipeline.placements_standard_dataset_join import (
    join_episodes_data,
)


def test_join_episodes_data():
    placements_standard = pd.DataFrame(
        {
            "child_ID": ["child-1", "child-1", "child-2", "unknown-child"],
            "placement_start_date": [
                date(2024, 1, 1),
                date(2024, 6, 1),
                date(2024, 4, 6),
                date(2024, 1, 1),
            ],
            "placement_type_offers": ["Foster", "Resi", "Foster", "Solo"],
        }
    )
    episodes = pd.DataFrame(
        {
            "CHILD": ["child-1", "child-1", "child-2"],
            "DECOM": [date(2024, 1, 1), date(2024, 6, 1), date(2024, 4, 6)],
            "EPISODE_ID": ["episode-1", "episode-2", "episode-3"],
        }
    )

    expected = pd.DataFrame(
        {
            "child_ID": ["child-1", "child-1", "child-2", "unknown-child"],
            "placement_start_date": [
                date(2024, 1, 1),
                date(2024, 6, 1),
                date(2024, 4, 6),
                date(2024, 1, 1),
            ],
            "placement_type_offers": ["Foster", "Resi", "Foster", "Solo"],
            "EPISODE_ID": ["episode-1", "episode-2", "episode-3", np.nan],
        }
    )

    result = join_episodes_data(episodes, placements_standard)

    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize(
    ("placement_start_date", "decom", "expected_episode_id"),
    [
        pytest.param(date(2024, 4, 4), date(2024, 4, 4), "episode-1", id="exact-date-match"),
        pytest.param(date(2024, 4, 3), date(2024, 4, 4), None, id="day-before-no-match"),
        pytest.param(date(2024, 4, 5), date(2024, 4, 4), None, id="day-after-no-match"),
    ],
)
def test_join_episodes_data_requires_exact_date_match(placement_start_date, decom, expected_episode_id):
    placements_standard = pd.DataFrame(
        {"child_ID": ["child-1"], "placement_start_date": [placement_start_date]}
    )
    episodes = pd.DataFrame(
        {"CHILD": ["child-1"], "DECOM": [decom], "EPISODE_ID": ["episode-1"]}
    )

    result = join_episodes_data(episodes, placements_standard)

    assert len(result) == 1
    if expected_episode_id is None:
        assert pd.isna(result.at[0, "EPISODE_ID"])
    else:
        assert result.at[0, "EPISODE_ID"] == expected_episode_id


def test_join_episodes_data_no_matching_child_returns_nan():
    placements_standard = pd.DataFrame(
        {"child_ID": ["child-1"], "placement_start_date": [date(2024, 1, 1)]}
    )
    episodes = pd.DataFrame(
        {
            "CHILD": ["child-2"],
            "DECOM": [date(2024, 1, 1)],
            "EPISODE_ID": ["episode-1"],
        }
    )

    result = join_episodes_data(episodes, placements_standard)

    assert len(result) == 1
    assert pd.isna(result.at[0, "EPISODE_ID"])


def test_join_episodes_data_preserves_row_order_and_unmatched_rows():
    placements_standard = pd.DataFrame(
        {
            "child_ID": ["child-1", "child-2", "child-3"],
            "placement_start_date": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
            ],
        }
    )
    episodes = pd.DataFrame(
        {
            "CHILD": ["child-3", "child-1"],
            "DECOM": [date(2024, 3, 1), date(2024, 1, 1)],
            "EPISODE_ID": ["episode-3", "episode-1"],
        }
    )

    result = join_episodes_data(episodes, placements_standard)

    assert result["child_ID"].tolist() == ["child-1", "child-2", "child-3"]
    assert result["EPISODE_ID"].tolist()[0] == "episode-1"
    assert pd.isna(result["EPISODE_ID"].tolist()[1])
    assert result["EPISODE_ID"].tolist()[2] == "episode-3"