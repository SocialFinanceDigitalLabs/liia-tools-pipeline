from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict


class ColumnConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    type: str
    unique_key: bool = False
    enrich: str | list = None
    enrich_input: str = None
    degrade: str = None
    sort: int = None
    asc: bool = False
    exclude: List[str] = []


class TableConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    sheetname: Optional[str] = None
    retain: List[str] = []
    columns: List[ColumnConfig]

    def __getitem__(self, value) -> ColumnConfig:
        ix = {t.id: t for t in self.columns}
        return ix[value]

    @property
    def sort_keys(self) -> List[Tuple[str, bool]]:
        """
        Returns a list of (column id, ascending) tuples that should be used to sort the table in order of priority
        """
        sort_tuples = [
            (c.id, c.asc, c.sort) for c in self.columns if c.sort is not None
        ]
        sort_tuples.sort(key=lambda x: x[2])
        return [(col_id, asc) for col_id, asc, _ in sort_tuples]

    def columns_for_profile(self, profile: str | list[str]) -> List[ColumnConfig]:
        '''
        If a profile is passed, keep columns that don't mention this profile in "exclude"
        If a list of profiles is passed, keep columns that aren't excluded for all these profiles
        If an empty list of profiles is passed, all cols will be dropped
        '''
        if isinstance(profile, str):
            return [c for c in self.columns if profile not in c.exclude]
        elif isinstance(profile, list):
            return [c for c in self.columns if not set(getattr(c, "exclude", [])).issuperset(set(profile))]
        raise TypeError


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sensor_trigger: Dict
    retention_columns: Dict
    retention_period: Dict
    degrade_at_clean: Dict
    reports_to_shared: Dict
    la_signed: Dict
    table_list: List[TableConfig]

    def __getitem__(self, value) -> TableConfig:
        ix = {t.id: t for t in self.table_list}
        return ix[value]

    def tables_for_profile(self, profile: str | list[str]) -> List[TableConfig]:
        '''
        If a single profile is passed, tables that match this profile in the retain options will be kept
        If a list of profiles is passed, any intersection between the list and retain will mean a table is kept
        '''
        if isinstance(profile, str):
            return [t for t in self.table_list if profile in t.retain]
        elif isinstance(profile, list):
            return [t for t in self.table_list if bool(set(t.retain) & set(profile))]
        raise TypeError
