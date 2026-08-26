"""Manual entrypoint for generating dataset header/node markdown docs.

Usage:
    python -m liiatools.common.generate_dataset_docs --dataset all
    python -m liiatools.common.generate_dataset_docs --dataset all --end-year 2027
    python -m liiatools.common.generate_dataset_docs --dataset annex_a
    python -m liiatools.common.generate_dataset_docs --dataset ssda903
    python -m liiatools.common.generate_dataset_docs --dataset school_census
    python -m liiatools.common.generate_dataset_docs --dataset pnw_census
    python -m liiatools.common.generate_dataset_docs --dataset cin_census
"""

from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path

from liiatools.annex_a_pipeline.spec import generate_schema_header_readme as generate_annex_a
from liiatools.cin_census_pipeline.spec import generate_schema_node_readme as generate_cin_nodes
from liiatools.pnw_census_pipeline.spec import generate_schema_header_readme as generate_pnw
from liiatools.school_census_pipeline.spec import generate_schema_header_readme as generate_school
from liiatools.ssda903_pipeline.spec import generate_schema_header_readme as generate_ssda903


def generate_docs(dataset: str = "all", end_year: int | None = None) -> list[Path]:
    """Generate one or more dataset docs and return the written file paths.

    Args:
        dataset: Which dataset to generate. Use "all" for every dataset.
        end_year: Optional shared end-year override for all year-based datasets.
            Start years stay at each dataset's default.
    """

    outputs: list[Path] = []

    if dataset in ("all", "annex_a"):
        outputs.append(generate_annex_a())

    if dataset in ("all", "ssda903"):
        if end_year is None:
            outputs.append(generate_ssda903())
        else:
            outputs.append(generate_ssda903(end_year=end_year))

    if dataset in ("all", "school_census"):
        if end_year is None:
            outputs.append(generate_school())
        else:
            outputs.append(generate_school(end_year=end_year))

    if dataset in ("all", "pnw_census"):
        if end_year is None:
            outputs.append(generate_pnw())
        else:
            outputs.append(generate_pnw(end_year=end_year))

    if dataset in ("all", "cin_census"):
        if end_year is None:
            outputs.append(generate_cin_nodes())
        else:
            outputs.append(generate_cin_nodes(end_year=end_year))

    return outputs


def main() -> None:
    """Parse CLI args and generate selected dataset documentation."""

    parser = ArgumentParser(description="Generate dataset header/node markdown docs")
    parser.add_argument(
        "--dataset",
        choices=["all", "annex_a", "ssda903", "school_census", "pnw_census", "cin_census"],
        default="all",
        help="Dataset doc to generate. Defaults to all.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Optional shared end year for all year-based datasets.",
    )

    args = parser.parse_args()
    output_paths = generate_docs(dataset=args.dataset, end_year=args.end_year)

    for output_path in output_paths:
        print(f"Generated {output_path}")


if __name__ == "__main__":
    main()
