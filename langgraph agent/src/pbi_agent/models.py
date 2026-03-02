from typing import Any, TypedDict

from pydantic import BaseModel, Field


class FilterSpec(BaseModel):
    name: str = Field(description="Filter name from the filter glossary.")
    op: str = Field(
        description="Filter operation: '=', '!=', 'in', 'between', '>=', '<='."
    )
    value: Any = Field(
        description="Filter value. Use string for single values or list for 'in'/'between'."
    )


class KPIQuery(BaseModel):
    kpi_name: str = Field(
        description="The KPI name requested by the user, or UNKNOWN if unclear."
    )
    filters: list[FilterSpec] = Field(
        default_factory=list,
        description="Optional filters extracted from the question.",
    )
    query_type: str = Field(
        default="kpi",
        description="Either 'kpi' for single value or 'topn' for ranked lists.",
    )
    group_by: str | None = Field(
        default=None,
        description="Filter name to group by for topn queries.",
    )
    limit: int = Field(
        default=5,
        description="Number of rows for topn queries.",
    )
    order: str = Field(
        default="DESC",
        description="Sort order for ranked lists: 'DESC' (top) or 'ASC' (bottom).",
    )
    table: str | None = Field(
        default=None,
        description="Table name for detail queries.",
    )
    columns: list[str] = Field(
        default_factory=list,
        description="Column names for detail queries (use filter glossary names).",
    )


class AgentState(TypedDict):
    question: str
    kpi_name: str
    dax_query: str
    filters: list[FilterSpec]
    skip_defaults: bool
    rejected_filters: list[str]
    query_type: str
    group_by: str | None
    limit: int
    order: str
    table: str | None
    columns: list[str]
    query_result: dict
    response: str
    error: str
    comparison_measures: list[dict] | None
    comparison_kpis: list[dict] | None
    chart_type: str | None
