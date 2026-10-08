import logging
import re
from functools import cached_property
from typing import Any, Dict, Iterable, List, Literal, Optional, Pattern

from pydantic import BaseModel, ConfigDict, Field

from liiatools.common.matching import normalise_text

log = logging.getLogger(__name__)


class Category(BaseModel):
    """
    Represents a categorical value in a column. It can be looked up by name or code by using the `in` operator,

    e.g. `if "B" in category: ...` or `if "Banana" in category: ...`
    """

    model_config = ConfigDict(extra="forbid")

    code: str
    name: str | list | None = None
    cell_regex: Any = None

    values: str = Field("")

    def __init__(self, **data):
        super().__init__(**data)

    def __contains__(self, item):
        # Schema values are stripped so stray whitespace in the schema can't break matching
        names = [self.name] if isinstance(self.name, str) else self.name or []
        values = {v.strip().lower() for v in [self.code, *names]}
        is_numeric = any(v.isnumeric() for v in values)

        if item in values:
            return True

        # If one of the categories are numeric, then we try to see if we can convert the item to a number if we didn't get any direct hits
        if is_numeric:
            try:
                int_value = str(int(float(item)))
                if int_value in values:
                    return True
            except (TypeError, ValueError):
                pass

        return False

    # Cached set of normalised codes and names for category types to save normalising with every cell
    @cached_property
    def normalised_values(self) -> set:
        names = [self.name] if isinstance(self.name, str) else self.name or []
        return {n for n in map(normalise_text, [self.code, *names]) if n}

    def matches_normalised(self, item) -> bool:
        """Relaxed match ignoring case, spacing and punctuation differences."""
        normalised = normalise_text(item)
        return bool(normalised) and normalised in self.normalised_values


class Numeric(BaseModel):
    """
    Represents a numeric value in a column, including the minimum value, maximum value and decimal places, flags for age verification
    """

    model_config = ConfigDict(extra="forbid")

    type: Literal["integer", "float"]
    min_value: int | None = None
    max_value: int | None = None
    decimal_places: int | None = None
    age: bool = False

    def __init__(self, **data):
        super().__init__(**data)


class Column(BaseModel):
    model_config = ConfigDict(extra="forbid")

    string: Literal["alphanumeric", "postcode", "regex"] | None = None
    numeric: Numeric | None = None
    date: str | None = None
    time: str | None = None

    dictionary: Dict | None = None
    category: List[Category] | None = None

    header_regex: Any = None
    cell_regex: Any = None

    canbeblank: bool = True

    @property
    def type(self):
        if self.string == "alphanumeric":
            return "string"
        elif self.string == "postcode":
            return "postcode"
        elif self.string == "regex":
            return "regex"
        elif self.numeric:
            return "numeric"
        elif self.date:
            return "date"
        elif self.time:
            return "time"
        elif self.category:
            return "category"
        else:
            raise ValueError("Unknown data type")

    @staticmethod
    def resolve_flags(flags: str) -> int:
        __flag_resolved = dict(i=re.I, m=re.M, s=re.S, u=re.U, l=re.L, x=re.X)
        flag_expr = 0
        if flags is not None:
            for flag in flags:
                flag_expr = flag_expr | __flag_resolved[flag]
        return flag_expr

    def parse_regex(self, regex: str) -> Pattern:
        """
        Parse a regex pattern '/{pattern}/{modifiers}'

        :param regex: regex expression
        :return: Compiled regex
        """
        separator = re.escape(regex[0])
        pattern = re.compile(
            "{separator}(.+)({separator}([imsulx]+)?)".format(separator=separator)
        )
        match = pattern.match(regex)
        if match is None:
            raise Exception("Failed to parse regular expression: '{}'".format(regex))

        pattern = match.group(1)
        flags = match.group(3)
        flags = self.resolve_flags(flags)

        return re.compile(pattern, flags)

    def match_category(self, value: str) -> Optional[Category]:
        '''
        Tries three ways to match a category value to the set of values in the config
        1. Exact match of stripped, lower case value with code or name
        2. Regex using category-specific regex in schema if exists
        3. Match of normalised value against normalised code or name
        '''
        assert self.category, "Column is not a category"

        value = value.strip().lower()
        for category in self.category:
            if value in category:
                return category.code
            elif category.cell_regex:
                for regex in category.cell_regex:
                    parse = self.parse_regex(regex)
                    if parse.match(value) is not None:
                        return category.code

        # Relaxed fallback: only accepted when it identifies exactly one category
        relaxed = {c.code for c in self.category if c.matches_normalised(value)}
        if len(relaxed) == 1:
            code = relaxed.pop()
            log.debug("Category value '%s' matched '%s' after normalisation", value, code)
            return code
        return None


class DataSchema(BaseModel):
    column_map: Dict[str, Dict[str, Column]]

    def get_table_from_headers(self, headers: Iterable[str]) -> Optional[str]:
        """
        Given a set of column names, finds the first table where all the table columns are contained
        with the headers.

        If no table is found, returns None
        """
        headers = set(headers)
        # Strip out falsy values like None and ""
        headers = {h for h in headers if h}
        for table_name, table_config in self.column_map.items():
            matched_columns = []

            # Creates a list of tuples holding (column_name, column_regex_list) for each configured column
            header_config = [
                (name, config.header_regex) for name, config in table_config.items()
            ]

            for actual_column in headers:
                # Check the actual value against each of the configured columns and store values
                header_matches = [
                    self.match_column_name(actual_column, c[0], c[1])
                    for c in header_config
                ]

                # Filter checks to only those that matched
                matching_configs = [c for c in header_matches if c is not None]

                # Relaxed fallback only when no exact/regex match exists
                if not matching_configs:
                    matching_configs = [
                        c[0]
                        for c in header_config
                        if self.match_column_name(actual_column, c[0], relaxed=True)
                    ]
                    if matching_configs:
                        log.info(
                            "Header '%s' matched %s after normalisation",
                            actual_column,
                            matching_configs,
                        )

                # Check if we have one or multiple configurations that match the actual value
                if len(matching_configs) == 1:
                    matched_columns.append(matching_configs[0])
                elif len(matching_configs) > 1:
                    raise ValueError(
                        "The actual column name matched multiple configured columns"
                    )

            # If all the expected columns are present, then we have a match
            if set(table_config.keys()) - set(matched_columns) == set():
                return table_name
        return None

    @property
    def table(self) -> Dict[str, Dict[str, Column]]:
        """
        Alias for column_map to make code a bit more readable
        """
        return self.column_map

    @staticmethod
    def match_column_name(
        actual_value: str,
        expected_value: str,
        expected_expressions: str = None,
        relaxed: bool = False,
    ) -> Optional[str]:
        """
        Matches an actual column name against an expected values. Can optionally take a list of expressions to test.
        Returns the expected value if a match is found, or None if no match is found.
        :param actual_value: Value that exists currently
        :param expected_value: Value that we expect
        :param expected_expressions: Optional list of (regex) expressions to test as well
        :param relaxed: Compare only after normalising case, spacing and punctuation (skips regexes)
        :return: The expected Value or None
        """

        if relaxed:
            normalised = normalise_text(actual_value)
            if normalised and normalised == normalise_text(expected_value):
                return expected_value
            return None

        actual_value = actual_value.lower().strip()
        test_expected_value = expected_value.lower().strip()

        if actual_value == test_expected_value:
            return expected_value

        if expected_expressions:
            for regex in expected_expressions:
                regex = Column().parse_regex(regex)
                if regex.match(actual_value):
                    return expected_value

        return None
