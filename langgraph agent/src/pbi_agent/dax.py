import re


def normalize_date_literal(value: str) -> str:
    match = re.match(r"^\s*(\d{4})-(\d{2})-(\d{2})\s*$", value)
    if not match:
        return ""
    year, month, day = match.groups()
    return f"DATE({int(year)}, {int(month)}, {int(day)})"


def escape_dax_string(value: str) -> str:
    return value.replace('"', '""')


def build_kpi_dax(measure: str) -> str:
    measure = (measure or "").strip()
    if not measure:
        return ""
    return f'EVALUATE ROW("KPI", {measure})'


def build_filtered_dax(measure: str, filter_expressions: list[str]) -> str:
    measure = (measure or "").strip()
    if not measure:
        return ""
    if not filter_expressions:
        return f'EVALUATE ROW("KPI", {measure})'
    joined_filters = ", ".join(filter_expressions)
    return f'EVALUATE ROW("KPI", CALCULATE({measure}, {joined_filters}))'


def build_topn_dax(
    measure: str,
    group_by_column: str,
    filter_expressions: list[str],
    limit: int,
    order: str = "DESC",
) -> str:
    measure = (measure or "").strip()
    if not measure or not group_by_column:
        return ""
    limit = max(1, min(int(limit or 5), 100))
    order = order.upper() if order else "DESC"
    filters = ", ".join(filter_expressions) if filter_expressions else ""
    if filters:
        summarize = (
            f"SUMMARIZECOLUMNS({group_by_column}, {filters}, \"KPI\", {measure})"
        )
    else:
        summarize = f"SUMMARIZECOLUMNS({group_by_column}, \"KPI\", {measure})"
    # Add deterministic tiebreak on the group-by column to match Power BI visuals
    # when many categories share the same KPI value (common for OTP% = 100%).
    return f"EVALUATE TOPN({limit}, {summarize}, [KPI], {order}, {group_by_column}, ASC)"


def build_grouped_dax(
    measure: str,
    group_by_column: str,
    filter_expressions: list[str],
    limit: int | None = None,
    order: str = "DESC",
) -> str:
    measure = (measure or "").strip()
    if not measure or not group_by_column:
        return ""
    order = order.upper() if order else "DESC"
    filters = ", ".join(filter_expressions) if filter_expressions else ""
    if filters:
        summarize = (
            f"SUMMARIZECOLUMNS({group_by_column}, {filters}, \"KPI\", {measure})"
        )
    else:
        summarize = f"SUMMARIZECOLUMNS({group_by_column}, \"KPI\", {measure})"
    if limit:
        limit = max(1, min(int(limit), 500))
        return f"EVALUATE TOPN({limit}, {summarize}, [KPI], {order}, {group_by_column}, ASC)"
    return f"EVALUATE {summarize}"


def build_multi_measure_dax(
    measures: list[tuple[str, str]],
    group_by_column: str,
    filter_expressions: list[str],
    limit: int | None = None,
    order: str = "ASC",
) -> str:
    """Build DAX query for multiple measures grouped by a dimension (e.g., time-series charts)"""
    if not measures or not group_by_column:
        return ""

    measure_columns = ", ".join([f'"{name}", {expr}' for name, expr in measures])
    order = order.upper() if order else "ASC"

    if filter_expressions:
        filters = ", ".join(filter_expressions)
        summarize = (
            f"SUMMARIZECOLUMNS({group_by_column}, {filters}, {measure_columns})"
        )
    else:
        summarize = f"SUMMARIZECOLUMNS({group_by_column}, {measure_columns})"

    if limit:
        limit = max(1, min(int(limit), 1000))
        return f"EVALUATE TOPN({limit}, {summarize}, {group_by_column}, {order})"
    return f"EVALUATE {summarize}"


def build_detail_dax(
    table_name: str,
    columns: list[str],
    filter_expressions: list[str],
    limit: int = 50,
    order_by: str | None = None,
) -> str:
    if not table_name or not columns:
        return ""
    limit = max(1, min(int(limit or 50), 1000))

    def _alias_from_expression(expression: str) -> str:
        match = re.search(r"\[([^\]]+)\]", expression)
        if match:
            return match.group(1).strip()
        return expression.strip()

    select_parts: list[str] = []
    for expr in columns:
        alias = _alias_from_expression(expr)
        select_parts.append(f'"{alias}", {expr}')
    select_columns = ", ".join(select_parts)

    filters = ", ".join(filter_expressions) if filter_expressions else ""
    if filters:
        base_table = f"CALCULATETABLE({table_name}, {filters})"
    else:
        base_table = table_name
    base = f"SELECTCOLUMNS({base_table}, {select_columns})"

    if order_by:
        return f"EVALUATE TOPN({limit}, {base}, {order_by}, ASC)"
    return f"EVALUATE TOPN({limit}, {base})"
