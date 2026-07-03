import importlib.resources
import logging
from functools import lru_cache
from pathlib import Path
from xml.etree import ElementTree as ET

import xmlschema
import yaml
from pydantic_yaml import parse_yaml_file_as

from liiatools.common.data import PipelineConfig
from liiatools.common.file_header_readme_generator import (
    MarkdownSection,
    render_readme_by_year,
    write_markdown_file,
)
from liiatools.common.spec import load_region_env

logger = logging.getLogger(__name__)

SCHEMA_DIR = Path(__file__).parent

region_config = load_region_env()


@lru_cache
def load_pipeline_config():
    try:
        with importlib.resources.open_text(
            f"{region_config}_pipeline_config", "cin_census_pipeline.json"
        ) as f:
            return parse_yaml_file_as(PipelineConfig, f)
    except ModuleNotFoundError:
        logger.info(f"Configuration region '{region_config}' not found.")
    except FileNotFoundError:
        logger.info(
            f"Configuration file 'cin_census_pipeline.json' not found in '{region_config}'."
        )


@lru_cache
def load_schema(year: int) -> (xmlschema.XMLSchema, Path):
    return (
        xmlschema.XMLSchema(SCHEMA_DIR / f"CIN_schema_{year:04d}.xsd"),
        Path(SCHEMA_DIR, f"CIN_schema_{year:04d}.xsd"),
    )


@lru_cache
def load_reports():
    with open(SCHEMA_DIR / "reports.yml", "rt") as FILE:
        return yaml.load(FILE, Loader=yaml.FullLoader)


def _extract_xsd_node_paths(schema_path: Path) -> list[tuple[str, bool]]:
    """Return a flat list of (node_path, required) from an XSD file."""
    ns = {"xs": "http://www.w3.org/2001/XMLSchema"}
    tree = ET.parse(schema_path)
    root = tree.getroot()

    # Build lookup for named complex types for type-based traversal.
    complex_types = {
        el.attrib["name"]: el
        for el in root.findall("xs:complexType", ns)
        if "name" in el.attrib
    }

    def iter_child_elements(node: ET.Element):
        for container in node.findall("xs:sequence", ns):
            for child in container.findall("xs:element", ns):
                yield child
        for container in node.findall("xs:all", ns):
            for child in container.findall("xs:element", ns):
                yield child
        for container in node.findall("xs:choice", ns):
            for child in container.findall("xs:element", ns):
                yield child

    paths: list[tuple[str, bool]] = []

    def walk_element(element: ET.Element, path_prefix: str, parent_required: bool, seen_types: set[str]):
        name = element.attrib.get("name")
        if not name:
            return

        min_occurs = int(element.attrib.get("minOccurs", "1"))
        required = parent_required and min_occurs > 0
        path = f"{path_prefix}/{name}" if path_prefix else name
        paths.append((path, required))

        # Inline complex type children.
        inline_complex_type = element.find("xs:complexType", ns)
        if inline_complex_type is not None:
            for child in iter_child_elements(inline_complex_type):
                walk_element(child, path, required, seen_types)

        # Named complex type children.
        type_name = element.attrib.get("type")
        if type_name:
            type_name = type_name.split(":")[-1]
            if type_name in complex_types and type_name not in seen_types:
                seen_types = set(seen_types)
                seen_types.add(type_name)
                for child in iter_child_elements(complex_types[type_name]):
                    walk_element(child, path, required, seen_types)

    top_level_elements = root.findall("xs:element", ns)
    for top_level in top_level_elements:
        walk_element(top_level, "", True, set())

    # Keep the first occurrence of each path in declaration order.
    deduped_paths: list[tuple[str, bool]] = []
    seen_paths: set[str] = set()
    for node_path, required in paths:
        if node_path not in seen_paths:
            seen_paths.add(node_path)
            deduped_paths.append((node_path, required))
    return deduped_paths


def render_schema_node_readme(start_year: int = 2017, end_year: int = 2026) -> str:
    years = list(range(start_year, end_year + 1))
    if not years:
        raise ValueError("No years to render")

    year_sections: dict[int, list[MarkdownSection]] = {}
    for year in years:
        _, schema_path = load_schema(year)
        node_paths = _extract_xsd_node_paths(schema_path)
        required_paths = [node for node, is_required in node_paths if is_required]
        optional_paths = [node for node, is_required in node_paths if not is_required]

        year_sections[year] = [
            MarkdownSection(
                summary=f"Required nodes ({len(required_paths)})",
                items=required_paths,
            ),
            MarkdownSection(
                summary=f"Optional nodes ({len(optional_paths)})",
                items=optional_paths,
            ),
        ]

    return render_readme_by_year(
        title="CIN Census Expected XML Nodes by Year",
        intro_lines=[
            "Required nodes are mandatory for a valid submission, while optional nodes may be included if applicable.",
        ],
        years=years,
        year_sections=year_sections,
    )


def generate_schema_node_readme(
    output_file: Path | None = None,
    start_year: int = 2017,
    end_year: int = 2026,
) -> Path:
    if output_file is None:
        output_file = Path("docs", "file_headers_by_dataset", "cin_census_nodes.md")

    readme_content = render_schema_node_readme(start_year=start_year, end_year=end_year)
    return write_markdown_file(output_file, readme_content)