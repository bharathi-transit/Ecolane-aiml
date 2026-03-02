# Power BI LangGraph Agent

A production-ready LangGraph agent that answers Power BI KPI questions, supports chart and comparison queries, and applies report defaults and filter rules.

## Structure
- `src/pbi_agent/` – core package
  - `agent.py` – LangGraph build/run logic
  - `cli.py` – interactive CLI
  - `config.py` – environment configuration loader
  - `dax.py` – DAX query builders
  - `filters.py` – filter handling and date relationship logic
  - `glossary.py` – KPI/filter glossary loaders + matching
  - `models.py` – Pydantic/Typed models
  - `parsing.py` – query parsing and template detection
  - `text_utils.py` – text normalization
  - `tools.py` – Power BI REST client
  - `utils.py` – formatting helpers
- `docs/` – reference docs
- `reports/` – generated artifacts

## Setup
1. Copy `.env.example` to `.env` and fill in credentials.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

## Run
```bash
python main.py
```

## Docs
- `docs/POWERBI_SETUP_GUIDE.md`
- `docs/ECOLANE_POWERBI_REFERENCE.md`
- `docs/data_model.md`
- `docs/SAMPLE_QUESTIONS.md`
