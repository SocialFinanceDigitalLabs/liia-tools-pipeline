from datetime import date

import numpy as np
import pandas as pd
import pytest

from liiatools.cans_pipeline.cans_dataset_join import join_episodes_data


@pytest.mark.parametrize(
    ("assessment_date", "episode_end", "expected_episode_id"),
    [
        pytest.param("03/04/2024", "06/04/2024", None, id="before-start"),
        pytest.param("04/04/2024", "06/04/2024", "episode-1", id="on-start"),
        pytest.param("05/04/2024", "06/04/2024", "episode-1", id="within-episode"),
        pytest.param("06/04/2024", "06/04/2024", None, id="on-end"),
        pytest.param("07/04/2024", "06/04/2024", None, id="after-end"),
        pytest.param("03/04/2024", np.nan, None, id="before-ongoing-episode"),
        pytest.param("04/04/2024", np.nan, "episode-1", id="on-ongoing-start"),
        pytest.param("05/04/2025", np.nan, "episode-1", id="within-ongoing-episode"),
    ],
)
def test_join_episodes_data_date_boundaries(assessment_date, episode_end, expected_episode_id):
    cans = pd.DataFrame({"Child Unique ID": ["child-1"], "Assessment Date": [assessment_date]})
    episodes = pd.DataFrame(
        {
            "CHILD": ["child-1"],
            "DECOM": ["04/04/2024"],
            "DEC": [episode_end],
            "EPISODE_ID": ["episode-1"],
        }
    )

    result = join_episodes_data(cans, episodes)

    assert len(result) == 1
    if expected_episode_id is None:
        assert pd.isna(result.at[0, "EPISODE_ID"])
    else:
        assert result.at[0, "EPISODE_ID"] == expected_episode_id


def test_join_episodes_data():
    cans = pd.DataFrame(
        {
            "Child Unique ID": ["child-1", "unknown-child", "child-1", "child-2", "child-1", "child-1"],
            "Assessment Date": ["05/04/2024"] * 4 + ["01/03/2024", "05/04/2024"],
            "Score": [1, 2, 3, 4, 5, 1],
        },
        index=[10, 20, 30, 40, 50, 60],
    )
    episodes = pd.DataFrame(
        {
            "CHILD": ["child-1", "child-2", "child-1"],
            "DECOM": ["04/04/2024", "01/04/2024", "01/01/2024"],
            "DEC": [np.nan, "06/04/2024", "01/02/2024"],
            "EPISODE_ID": ["1", "2", "3"],
        }
    )
    expected = pd.DataFrame(
        {
            "Child Unique ID": ["child-1", "unknown-child", "child-1", "child-2", "child-1", "child-1"],
            "Assessment Date": [date(2024, 4, 5)] * 4 + [date(2024, 3, 1), date(2024, 4, 5)],
            "Score": [1, 2, 3, 4, 5, 1],
            "EPISODE_ID": [
                "1",
                np.nan,
                "1",
                "2",
                np.nan,
                "1",
            ],
        }
    )

    result = join_episodes_data(cans, episodes)

    pd.testing.assert_frame_equal(result, expected)
