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

    def columns_for_profile(self, profile: str) -> List[ColumnConfig]:
        return [c for c in self.columns if profile not in c.exclude]


class PipelineConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sensor_trigger: Dict
    retention_columns: Dict
    retention_period: Dict
    reports_to_shared: Dict
    la_signed: Dict
    table_list: List[TableConfig]

    def __getitem__(self, value) -> TableConfig:
        ix = {t.id: t for t in self.table_list}
        return ix[value]

    def tables_for_profile(self, profile: str) -> List[TableConfig]:
        return [t for t in self.table_list if profile in t.retain]
