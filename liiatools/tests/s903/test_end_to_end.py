import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

import liiatools
from liiatools.__main__ import cli
from liiatools.ssda903_pipeline.spec.samples import EPISODES_2020, HEADER_2020


@pytest.fixture(scope="session", autouse=True)
def liiatools_dir():
    return Path(liiatools.__file__).parent


@pytest.fixture(scope="session", autouse=True)
def build_dir(liiatools_dir):
    build_dir = liiatools_dir / "../build/tests/s903"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    return build_dir


@pytest.mark.skipif(os.environ.get("SKIP_E2E"), reason="Skipping end-to-end tests")
def test_end_to_end(build_dir):
    incoming_dir = build_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir = build_dir / "pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    for year in range(2020, 2024):
        year_dir = incoming_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)

        shutil.copy(EPISODES_2020, year_dir / f"BAR_SSDA903_{year}_episodes.csv")
        shutil.copy(HEADER_2020, year_dir / f"BAR_SSDA903_{year}_header.csv")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "s903",
            "pipeline",
            "--input-location",
            incoming_dir.as_posix(),
            "--output-location",
            pipeline_dir.as_posix(),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    shutil.rmtree(build_dir.parents[1])


@pytest.mark.skip("Old pipeline")
def test_end_to_end_old(liiatools_dir, build_dir, log_dir):
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "s903",
            "cleanfile",
            "--i",
            str(
                liiatools_dir
                / "ssda903_pipeline/spec/samples/SSDA903_2020_episodes.csv"
            ),
            "--o",
            str(build_dir),
            "--la_log_dir",
            str(log_dir),
            "--la_code",
            "BAD",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
