from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence


@dataclass
class MarkdownSection:
    """Represents one expandable markdown <details> section.

    Attributes:
        summary: The text shown in the <summary> row.
        items: Bullet list values rendered directly under the summary.
        children: Nested sections rendered after items.
        list_children: When True, wraps children in a <ul>...</ul> block.
    """

    summary: str
    items: list[str] = field(default_factory=list)
    children: list["MarkdownSection"] = field(default_factory=list)
    list_children: bool = False


def natural_sort_key(value: str) -> tuple[str, int, str]:
    """Return a sort key that places numeric suffixes in natural order.

    Example: list_2 comes before list_10.
    """

    match = re.match(r"^(.*?)(\d+)$", value)
    if match:
        return (match.group(1), int(match.group(2)), value)
    return (value, -1, value)


def _render_section(section: MarkdownSection, lines: list[str]) -> None:
    """Render one MarkdownSection (and its children) into markdown lines."""

    lines.append("<details>")
    lines.append(f"<summary>{section.summary}</summary>")
    lines.append("")

    for item in section.items:
        lines.append(f"- {item}")

    if section.items:
        lines.append("")

    if section.children:
        if section.list_children:
            lines.append("<ul>")
            for child in section.children:
                lines.append("<li>")
                _render_section(child, lines)
                lines.append("</li>")
                lines.append("")
            lines.append("</ul>")
        else:
            for child in section.children:
                _render_section(child, lines)

    lines.append("</details>")
    lines.append("")


def render_readme_by_year(
    title: str,
    intro_lines: Sequence[str],
    years: Sequence[int],
    year_sections: dict[int, list[MarkdownSection]],
    empty_year_message: str | None = None,
) -> str:
    """Build a markdown document that is organized by year.

    Args:
        title: Top-level markdown heading text.
        intro_lines: Introductory text rendered below the title.
        years: Years to render in the Year Index and body sections.
        year_sections: Mapping of year to section list for that year.
        empty_year_message: Optional message for years with no sections.
    """

    lines: list[str] = [f"# {title}", ""]
    lines.extend(intro_lines)
    lines.extend(["", "## Year Index", ""])

    for year in years:
        lines.append(f"- [{year}](#{year})")

    lines.append("")

    for year in years:
        lines.append(f"## {year}")
        lines.append("")

        sections = year_sections.get(year, [])
        if not sections:
            if empty_year_message:
                lines.append(empty_year_message)
                lines.append("")
            continue

        for section in sections:
            _render_section(section, lines)

    return "\n".join(lines).rstrip() + "\n"


def render_single_schema_readme(
    title: str,
    intro_lines: Sequence[str],
    sections: Sequence[MarkdownSection],
) -> str:
    """Build a markdown document for datasets that do not vary by year."""

    lines: list[str] = [f"# {title}", ""]
    lines.extend(intro_lines)
    lines.append("")

    for section in sections:
        _render_section(section, lines)

    return "\n".join(lines).rstrip() + "\n"


def write_markdown_file(output_file: Path, content: str) -> Path:
    """Write markdown content to disk, creating parent folders if needed."""

    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(content, encoding="utf-8")
    return output_file
