from typing import Any


def extract_kpi_value(result: dict) -> str:
    try:
        tables = result["results"][0]["tables"]
        table = tables[0]
        rows = table.get("rows", [])
        if not rows:
            return ""

        first_row = rows[0]
        if isinstance(first_row, dict):
            return str(first_row.get("KPI", next(iter(first_row.values()), "")))
        if isinstance(first_row, list) and first_row:
            return str(first_row[0])
    except Exception:
        return ""

    return ""


def format_number(value: Any) -> str:
    try:
        if isinstance(value, str):
            num = float(value.replace(",", ""))
        else:
            num = float(value)
        return f"{num:.2f}"
    except Exception:
        return str(value)


def float_safe(value: Any) -> float | str:
    try:
        if isinstance(value, str):
            return float(value.replace(",", ""))
        return float(value)
    except Exception:
        return str(value)
