import pandas as pd

from liiatools.cin_census_pipeline.dedup_cin import dedup_cin


def test_dedup_cin():
    # Test dataframe
    df = pd.DataFrame({
        "LAchildID": ["A", "A", "A", "A", "B", "B", "C"],
        "CINreferralDate": [
            "2024-01-01", "2024-01-01", "2024-01-01",  # A same referral date
            "2023-02-02",                              # A different referral date
            "2023-05-05", "2023-05-05",                # B same referral date
            "2022-09-09"                               # C unique
        ],
        "Year": [2023, 2023, 2024, 2023, 2022, 2023, 2022]
    })

    # Act
    result = dedup_cin(df)

    # --- Assertions ---
    # A, referral 2024-01-01 → only latest year (2024) should remain
    a_ref1 = result[(result["LAchildID"] == "A") & 
                    (result["CINreferralDate"] == "2024-01-01")]
    assert all(a_ref1["Year"] == 2024)
    assert len(a_ref1) == 1  # should only keep the newest row(s)

    # A, referral 2023-02-02 → no duplicates, should still be kept
    a_ref2 = result[(result["LAchildID"] == "A") & 
                    (result["CINreferralDate"] == "2023-02-02")]
    assert len(a_ref2) == 1
    assert a_ref2["Year"].iloc[0] == 2023

    # B, referral 2023-05-05 → only latest year (2023) should remain
    b_ref = result[(result["LAchildID"] == "B") & 
                   (result["CINreferralDate"] == "2023-05-05")]
    assert all(b_ref["Year"] == 2023)
    assert len(b_ref) == 1

    # C should still be present since it had no duplicates
    assert "C" in result["LAchildID"].values

    # Check overall row count: 
    # A(1 row from 2024, 1 row from alt ref in 2023) + B(1) + C(1) = 4 rows
    assert len(result) == 4
