import json
import os
import re
from difflib import SequenceMatcher

from .text_utils import normalize_text


def load_kpi_glossary(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, list):
        raise ValueError("KPI glossary must be a list of objects.")

    return data


def format_kpi_glossary(entries: list[dict]) -> str:
    if not entries:
        return "No KPI glossary entries provided."

    lines = []
    for entry in entries:
        name = entry.get("name", "").strip()
        measure = entry.get("measure", "").strip()
        definition = entry.get("definition", "").strip()
        synonyms = entry.get("synonyms", [])
        if isinstance(synonyms, list):
            synonyms_text = ", ".join([str(value).strip() for value in synonyms if value])
        else:
            synonyms_text = str(synonyms).strip()

        lines.append(
            f"- Name: {name}; Measure: {measure}; Definition: {definition}; Synonyms: {synonyms_text}"
        )

    return "\n".join(lines)


def iter_kpi_aliases(entry: dict) -> list[str]:
    aliases: list[str] = []
    name = entry.get("name", "")
    if name:
        aliases.append(str(name))
    measure_expr = entry.get("measure", "")
    match = re.search(r"\[([^\]]+)\]", measure_expr or "")
    if match:
        aliases.append(match.group(1).strip())
    synonyms = entry.get("synonyms", [])
    if isinstance(synonyms, list):
        aliases.extend([str(value) for value in synonyms if value])
    elif synonyms:
        aliases.append(str(synonyms))
    return aliases


def match_kpi_from_question(question: str, entries: list[dict]) -> dict | None:
    normalized_question = normalize_text(question)
    if not normalized_question:
        return None

    # Prefer exact KPI name matches before considering synonyms/measure aliases.
    name_matches = [
        entry
        for entry in entries
        if normalize_text(entry.get("name", "")) == normalized_question
    ]
    if len(name_matches) == 1:
        return name_matches[0]

    candidates: list[tuple[int, dict, str]] = []
    for idx, entry in enumerate(entries):
        for alias in iter_kpi_aliases(entry):
            alias_norm = normalize_text(alias)
            if alias_norm:
                candidates.append((idx, entry, alias_norm))

    exact_matches = {
        idx: entry
        for idx, entry, alias_norm in candidates
        if alias_norm == normalized_question
    }
    if len(exact_matches) == 1:
        return next(iter(exact_matches.values()))

    substring_hits: list[tuple[int, dict, int]] = []
    for idx, entry, alias_norm in candidates:
        if alias_norm and alias_norm in normalized_question:
            substring_hits.append((idx, entry, len(alias_norm)))

    if substring_hits:
        substring_hits.sort(key=lambda item: item[2], reverse=True)
        best_len = substring_hits[0][2]
        best_entries = {idx for idx, _, length in substring_hits if length == best_len}
        if len(best_entries) == 1:
            return substring_hits[0][1]

    best_score = 0.0
    best_entry: dict | None = None
    best_idx: int | None = None
    for idx, entry, alias_norm in candidates:
        score = SequenceMatcher(None, normalized_question, alias_norm).ratio()
        if score > best_score:
            best_score = score
            best_entry = entry
            best_idx = idx
        elif score == best_score and idx != best_idx:
            best_entry = None

    if best_score >= 0.88 and best_entry:
        return best_entry

    return None


def match_kpi_by_measure(measure_name: str, entries: list[dict]) -> dict | None:
    target = normalize_text(measure_name)
    if not target:
        return None
    for entry in entries:
        measure = measure_name_from_expression(entry.get("measure", ""))
        if normalize_text(measure) == target:
            return entry
    return None


def load_filter_glossary(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, list):
        raise ValueError("Filter glossary must be a list of objects.")

    return data


def format_filter_glossary(entries: list[dict]) -> str:
    if not entries:
        return "No filter glossary entries provided."

    lines = []
    for entry in entries:
        name = entry.get("name", "").strip()
        data_type = entry.get("type", "").strip()
        synonyms = entry.get("synonyms", [])
        if isinstance(synonyms, list):
            synonyms_text = ", ".join([str(value).strip() for value in synonyms if value])
        else:
            synonyms_text = str(synonyms).strip()

        lines.append(
            f"- Name: {name}; Type: {data_type}; Synonyms: {synonyms_text}"
        )

    return "\n".join(lines)


def load_kpi_filter_map(path: str) -> dict:
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("KPI filter map must be a JSON object.")

    return data


def load_report_defaults(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, list):
        raise ValueError("Report defaults must be a list of objects.")

    return data


def load_page_defaults(path: str) -> dict:
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("Report page defaults must be a JSON object.")

    return data


def load_visual_map(path: str) -> dict:
    if not os.path.exists(path):
        return {}

    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if not isinstance(data, dict):
        raise ValueError("Visual map must be a JSON object.")

    return data


def measure_name_from_expression(measure_expression: str) -> str:
    if not measure_expression:
        return ""
    match = re.search(r"\[([^\]]+)\]", measure_expression)
    if match:
        return match.group(1).strip()
    return measure_expression
