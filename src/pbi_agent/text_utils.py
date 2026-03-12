import re


def normalize_text(text: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9\s]+", " ", text.lower())
    return re.sub(r"\s+", " ", cleaned).strip()
