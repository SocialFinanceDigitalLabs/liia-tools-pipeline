import os
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

import liiatools
from liiatools.__main__ import cli
from liiatools.cin_census_pipeline.spec.samples import CIN_2022


@pytest.fixture(scope="session", autouse=True)
def liiatools_dir():
    return Path(liiatools.__file__).parent


@pytest.fixture(scope="session", autouse=True)
def build_dir(liiatools_dir):
    build_dir = liiatools_dir / "../build/tests/cin_census"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)

    return build_dir


@pytest.fixture(scope="session", autouse=True)
def log_dir(build_dir):
    log_dir = build_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    return build_dir


@pytest.mark.skipif(os.environ.get("SKIP_E2E"), reason="Skipping end-to-end tests")
def test_end_to_end(build_dir):
    incoming_dir = build_dir / "incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir = build_dir / "pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy(CIN_2022, incoming_dir / f"BAR_cin_2022.xml")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "cin-census",
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
            "cin-census",
            "cleanfile",
            "--i",
            str(liiatools_dir / "cin_census_pipeline/spec/samples/BAR_cin_2022.xml"),
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

    la_out = build_dir / "la_out"
    la_out.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(
        cli,
        [
            "cin-census",
            "la-agg",
            "--i",
            str(build_dir / "cin-2022_clean.csv"),
            "--flat_output",
            str(la_out),
            "--analysis_output",
            str(la_out),
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    pan_out = build_dir / "pan_out"
    pan_out.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(
        cli,
        [
            "cin-census",
            "pan-agg",
            "--i",
            str(la_out / "CIN_Census_merged_flatfile.csv"),
            "--flat_output",
            str(pan_out),
            "--analysis_output",
            str(pan_out),
            "--la_code",
            "BAD",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
