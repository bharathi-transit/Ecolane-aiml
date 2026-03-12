import re
from datetime import date, timedelta

from .glossary import iter_kpi_aliases, match_kpi_by_measure
from .text_utils import normalize_text


def detect_comparison_chart_pattern(question: str, visual_map: dict) -> dict | None:
    """Detect if question matches a predefined comparison chart pattern"""
    normalized = normalize_text(question)
    if not normalized:
        return None

    patterns = visual_map.get("comparison_chart_patterns", {})
    if not isinstance(patterns, dict):
        return None

    for key, template in patterns.items():
        key_norm = normalize_text(key)
        if key_norm in normalized:
            return template

    if not re.search(r"\b(vs|versus|compare|compared|comparison|and)\b", normalized):
        return None

    for key, template in patterns.items():
        key_norm = normalize_text(key)
        if normalized in key_norm:
            return template

    question_words = set(normalized.split())
    best_match = None
    best_score = 0

    for key, template in patterns.items():
        pattern_words = set(normalize_text(key).split())
        common_words = question_words.intersection(pattern_words)
        score = len(common_words) / len(pattern_words) if pattern_words else 0

        if score > best_score and score >= 0.7:
            best_score = score
            best_match = template

    return best_match


def detect_comparison_query(question: str, entries: list[dict]) -> list[dict] | None:
    """Detect if question asks for comparison of multiple KPIs (e.g., 'Service vs Revenue Miles')"""
    normalized = normalize_text(question)

    comparison_patterns = [
        r"\bvs\b",
        r"\bversus\b",
        r"\bcompared to\b",
        r"\bcompare\b",
        r"\band\b.*\band\b",
    ]

    has_comparison = any(re.search(pattern, normalized) for pattern in comparison_patterns)
    if not has_comparison:
        return None

    split_pattern = r"\s+(?:vs|versus|compared to|compare|and)\s+"
    parts = re.split(split_pattern, normalized, flags=re.IGNORECASE)

    matched_kpis = []
    matched_indices = set()

    for part in parts:
        if not part.strip():
            continue

        part_normalized = part.strip()
        best_match = None
        best_score = 0
        best_idx = None

        for idx, entry in enumerate(entries):
            if idx in matched_indices:
                continue

            for alias in iter_kpi_aliases(entry):
                alias_norm = normalize_text(alias)
                if not alias_norm:
                    continue

                if alias_norm in part_normalized or part_normalized in alias_norm:
                    score = len(alias_norm) if alias_norm in part_normalized else len(part_normalized)
                    if score > best_score:
                        best_score = score
                        best_match = entry
                        best_idx = idx

        if best_match and best_idx is not None:
            matched_kpis.append(best_match)
            matched_indices.add(best_idx)

    if len(matched_kpis) >= 2:
        return matched_kpis

    return None


def parse_explicit_grouped_query(
    question: str, glossary_entries: list[dict], filter_entries: list[dict]
) -> dict | None:
    normalized = normalize_text(question)
    if "grouped by" not in normalized:
        return None

    measure_match = re.search(r"\[([^\]]+)\]", question)
    if not measure_match:
        return None
    measure_name = measure_match.group(1).strip()
    kpi_entry = match_kpi_by_measure(measure_name, glossary_entries)
    if not kpi_entry:
        return None

    column_match = re.search(r"'([^']+)'\s*\[([^\]]+)\]", question)
    if not column_match:
        return None
    table_name, column_name = column_match.groups()
    column_expr = f"'{table_name}'[{column_name}]"
    group_by_entry = next(
        (entry for entry in filter_entries if entry.get("column") == column_expr), None
    )
    if not group_by_entry:
        return None

    inferred_type = infer_query_type_from_text(question) or "barchart"
    return {
        "kpi_name": kpi_entry.get("name", ""),
        "filters": [],
        "query_type": inferred_type,
        "group_by": group_by_entry.get("name", ""),
        "limit": 10,
        "order": "DESC",
        "table": None,
        "columns": [],
        "skip_defaults": True,
    }


def detect_page(question: str, page_defaults: dict) -> str | None:
    normalized_question = normalize_text(question)
    if not normalized_question or not page_defaults:
        return None

    best_page = None
    highest_score = 0.0

    question_words = set(normalized_question.split())

    for page in page_defaults.keys():
        page_words = set(normalize_text(page).split())

        common_words = {"by", "and", "or", "to", "for", "of"}
        page_keywords = page_words - common_words

        if not page_keywords:
            continue

        matches = len(page_keywords.intersection(question_words))
        score = matches / len(page_keywords)

        if score > highest_score:
            highest_score = score
            best_page = page

    if highest_score > 0.6:
        return best_page

    return None


def detect_detail_template(question: str, visual_map: dict) -> dict | None:
    normalized = normalize_text(question)
    if not normalized:
        return None
    templates = visual_map.get("detail_defaults", {})
    if not isinstance(templates, dict):
        return None
    for key, template in templates.items():
        if normalize_text(key) == normalized:
            return {"__name__": key, **template}

    for key, template in templates.items():
        key_norm = normalize_text(key)
        if key_norm in normalized or normalized in key_norm:
            return {"__name__": key, **template}
    return None


def detect_chart_template(question: str, visual_map: dict) -> dict | None:
    normalized = normalize_text(question)
    if not normalized:
        return None
    templates = visual_map.get("chart_defaults", {})
    if not isinstance(templates, dict):
        return None

    for key, template in templates.items():
        if normalize_text(key) == normalized:
            return {"__name__": key, **template}

    matches: list[tuple[tuple[int, int], dict]] = []
    for key, template in templates.items():
        key_norm = normalize_text(key)
        if key_norm in normalized:
            matches.append(((2, len(key_norm)), {"__name__": key, **template}))
        elif normalized in key_norm:
            matches.append(((1, len(key_norm)), {"__name__": key, **template}))

    if not matches:
        return None
    matches.sort(key=lambda item: item[0], reverse=True)
    return matches[0][1]


def infer_query_type_from_text(question: str) -> str | None:
    text = normalize_text(question)
    if "top" in text or "highest" in text:
        return "topn"
    if "bottom" in text or "lowest" in text:
        return "topn"
    if "list" in text or "rank" in text:
        return "topn"
    if re.search(r"\bby\s+(hour|day|week|month|year|vehicle|driver|status|type|purpose|age|miles?|location|poi|agent|booking|funding|program|county|city|state)", text):
        return "barchart"
    # Don't infer a chart just because a KPI name contains "percent/percentage".
    if any(token in text for token in ["pie", "donut", "distribution", "share"]):
        return "pie"
    if any(token in text for token in ["details", "detail", "table", "list of", "show all"]):
        return "detail"
    return None


def is_explicit_topn_question(question: str) -> bool:
    text = normalize_text(question)
    if re.search(r"\b(top|bottom)\s+\d+\b", text):
        return True
    if re.search(r"\b(top|bottom)\b", text):
        return True
    if re.search(r"\bhighest\b|\blowest\b", text):
        return True
    if re.search(r"\brank\b|\branked\b|\branking\b", text):
        return True
    return False


def parse_relative_date_range(question: str, today: date) -> tuple[str, str] | None:
    text = normalize_text(question)
    if "today" in text:
        start = today
        end = today
    elif "yesterday" in text:
        start = today - timedelta(days=1)
        end = today - timedelta(days=1)
    elif "last week" in text:
        start = today - timedelta(days=7)
        end = today - timedelta(days=1)
    elif "last month" in text:
        start = today.replace(day=1) - timedelta(days=1)
        start = start.replace(day=1)
        end = today.replace(day=1) - timedelta(days=1)
    elif "this month" in text:
        start = today.replace(day=1)
        end = today
    elif "this week" in text:
        start = today - timedelta(days=today.weekday())
        end = today
    else:
        return None

    return (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
