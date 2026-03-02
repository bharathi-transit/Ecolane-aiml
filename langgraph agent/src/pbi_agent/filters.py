from difflib import SequenceMatcher
from typing import Any

from .dax import escape_dax_string, normalize_date_literal
from .models import FilterSpec
from .text_utils import normalize_text
from .tools import PowerBITool


def is_all_value(value: Any) -> bool:
    if isinstance(value, str):
        return normalize_text(value) in {"all", "(all)"}
    return False


def normalize_status_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    normalized = normalize_text(value)
    mapping = {
        "complete": "comp",
        "completed": "comp",
        "comp": "comp",
        "no show": "noshow",
        "noshow": "noshow",
        "no-show": "noshow",
        "cancelled": "cancel",
        "canceled": "cancel",
        "cancel": "cancel",
    }
    return mapping.get(normalized, value)


def is_archive_status_filter(filter_entry: dict) -> bool:
    return filter_entry.get("column") == "'ArchiveTrip'[status]"


def select_date_relationship(question: str, kpi_entry: dict | None) -> str | None:
    text = normalize_text(question)
    if any(token in text for token in ["requested", "booking", "booked", "ordered"]):
        return "requested_pickup"
    if any(token in text for token in ["actual", "occurred", "happened"]):
        return "actual_pickup_arrival"
    if any(token in text for token in ["promised", "scheduled"]):
        return "promised_pickup"
    if kpi_entry:
        name = normalize_text(kpi_entry.get("name", ""))
        measure = normalize_text(kpi_entry.get("measure", ""))
        if "otp" in name or "otp" in measure:
            return "promised_pickup"
    return None


def apply_date_relationship(
    measure_expr: str,
    relationship: str | None,
    date_column: str | None = None,
) -> str:
    if not measure_expr or not relationship:
        return measure_expr
    relationship_column = f"'ArchiveTrip'[{relationship}]"
    date_column = date_column or "'Date_time'[Date_Key]"
    return f"CALCULATE({measure_expr}, USERELATIONSHIP({relationship_column}, {date_column}))"


def detect_supported_relationships(pbi_tool: PowerBITool) -> dict[str, str]:
    """
    Detect which ArchiveTrip date columns are actually related to Date_time.
    Returns a mapping of relationship column -> date column to use.
    """
    candidates = ["requested_pickup", "promised_pickup", "actual_pickup_arrival"]
    date_columns = ["'Date_time'[Date_Key]", "'Date_time'[L_Date]"]
    supported: dict[str, str] = {}
    for rel in candidates:
        for date_col in date_columns:
            dax = (
                'EVALUATE ROW("Test", CALCULATE([Total Trips], '
                f"USERELATIONSHIP('ArchiveTrip'[{rel}], {date_col})))"
            )
            try:
                pbi_tool.execute_query(dax)
            except Exception:
                continue
            supported[rel] = date_col
            break
    return supported


def resolve_filter_name(name: str, filters: list[dict]) -> dict | None:
    if not name:
        return None
    normalized = normalize_text(name)
    if not normalized:
        return None

    for entry in filters:
        if normalize_text(entry.get("name", "")) == normalized:
            return entry

    for entry in filters:
        synonyms = entry.get("synonyms", [])
        if isinstance(synonyms, list):
            if any(normalize_text(str(syn)) == normalized for syn in synonyms):
                return entry
        elif synonyms and normalize_text(str(synonyms)) == normalized:
            return entry

    best_score = 0.0
    best_entry = None
    for entry in filters:
        score = SequenceMatcher(None, normalized, normalize_text(entry.get("name", ""))).ratio()
        if score > best_score:
            best_score = score
            best_entry = entry
    if best_score >= 0.9:
        return best_entry
    return None


def build_filter_expression(filter_entry: dict, spec: FilterSpec) -> list[str]:
    column = filter_entry.get("column", "")
    if not column:
        return []

    op = (spec.op or "").strip().lower()
    if op == "!=":
        op = "<>"
    data_type = (filter_entry.get("type") or "text").lower()
    value = spec.value
    if isinstance(value, list):
        cleaned = [v for v in value if not is_all_value(v)]
        if not cleaned:
            return []
        value = cleaned
    if is_all_value(value):
        return []
    if is_archive_status_filter(filter_entry):
        if isinstance(value, list):
            value = [normalize_status_value(v) for v in value]
        else:
            value = normalize_status_value(value)
    is_blank = isinstance(value, str) and normalize_text(value) in {"blank", "(blank)"}

    if data_type == "date":
        table_name = column.split("[", 1)[0].strip()
        if op == "between" and isinstance(value, list) and len(value) == 2:
            start = normalize_date_literal(str(value[0]))
            end = normalize_date_literal(str(value[1]))
            if start and end:
                return [f"FILTER({table_name}, {column} >= {start} && {column} <= {end})"]
        if op in {"=", ">=", "<="} and isinstance(value, str):
            date_literal = normalize_date_literal(value)
            if date_literal:
                return [f"FILTER({table_name}, {column} {op} {date_literal})"]
        return []

    if data_type in {"number", "boolean"}:
        if op == "between" and isinstance(value, list) and len(value) == 2:
            return [f"{column} >= {value[0]}", f"{column} <= {value[1]}"]
        if op in {"=", "!=", "<>", ">=", "<="}:
            if is_blank:
                return [f"{column} {op} BLANK()"]
            return [f"{column} {op} {value}"]
        if op in {"in", "not in"} and isinstance(value, list) and value:
            joined = ", ".join([
                ("BLANK()" if normalize_text(str(v)) in {"blank", "(blank)"} else str(v))
                for v in value
            ])
            clause = f"{column} IN {{ {joined} }}"
            if op == "not in":
                clause = f"NOT ({clause})"
            return [f"FILTER(ALL({column}), {clause})"]
        return []

    # text
    if op == "between":
        return []
    table_name = column.split("[", 1)[0].strip()
    if op in {"in", "not in"} and isinstance(value, list) and value:
        parts = []
        for v in value:
            if normalize_text(str(v)) in {"blank", "(blank)"}:
                parts.append("BLANK()")
            else:
                parts.append(f'"{escape_dax_string(str(v))}"')
        joined = ", ".join(parts)
        clause = f"{column} IN {{ {joined} }}"
        if op == "not in":
            clause = f"NOT ({clause})"
        return [f"FILTER(ALL({column}), {clause})"]
    if op in {"=", "!=", "<>"} and isinstance(value, str):
        if is_blank:
            clause = f"{column} {op} BLANK()"
        else:
            clause = f'{column} {op} "{escape_dax_string(value)}"'
        return [f"FILTER(ALL({column}), {clause})"]
    return []


def select_primary_date_filter(filters: list[dict]) -> dict | None:
    if not filters:
        return None
    candidates = [f for f in filters if (f.get("type") or "").lower() == "date"]
    if not candidates:
        return None
    preferred = ["date_key", "date", "trip date", "service date"]
    for pref in preferred:
        for entry in candidates:
            if pref in normalize_text(entry.get("name", "")):
                return entry
    return candidates[0]
