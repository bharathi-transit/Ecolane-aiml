import json
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from xml.sax.saxutils import escape


def _col_letter(idx: int) -> str:
    # 1-indexed
    if idx < 1:
        raise ValueError("idx must be >= 1")
    out = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        out = chr(ord("A") + rem) + out
    return out


def _cell(ref: str, value: str, numeric: bool = False) -> str:
    if numeric:
        return f'<c r="{ref}" t="n"><v>{escape(str(value))}</v></c>'
    return f'<c r="{ref}" t="inlineStr"><is><t>{escape(str(value))}</t></is></c>'


def _build_sheet_xml(rows: list[list[tuple[str, bool]]]) -> str:
    # rows: list of row, each cell is (value, numeric)
    n_rows = len(rows)
    n_cols = max((len(r) for r in rows), default=0)
    dim_end = f"{_col_letter(max(1, n_cols))}{max(1, n_rows)}"

    sheet_rows = []
    for r_idx, row in enumerate(rows, start=1):
        cells = []
        for c_idx, (val, numeric) in enumerate(row, start=1):
            ref = f"{_col_letter(c_idx)}{r_idx}"
            cells.append(_cell(ref, val, numeric=numeric))
        sheet_rows.append(f'<row r="{r_idx}">' + "".join(cells) + "</row>")

    return (
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<dimension ref="A1:{dim_end}" />'
        '<sheetViews><sheetView workbookViewId="0">'
        '<selection activeCell="A1" sqref="A1" />'
        "</sheetView></sheetViews>"
        '<sheetFormatPr baseColWidth="8" defaultRowHeight="15" />'
        "<sheetData>"
        + "".join(sheet_rows)
        + "</sheetData>"
        "</worksheet>"
    )


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    os.chdir(project_root)
    sys.path.insert(0, str(project_root))

    glossary_path = project_root / "src" / "pbi_agent" / "kpi_glossary.json"
    template_xlsx = project_root / "reports" / "KPI_Calculations.xlsx"
    out_xlsx = project_root / "reports" / "KPI_Calculations.xlsx"

    if not glossary_path.exists():
        raise FileNotFoundError(f"Missing {glossary_path}")
    if not template_xlsx.exists():
        raise FileNotFoundError(
            f"Missing template {template_xlsx}. Run once to create it, or add a template file."
        )

    from src.pbi_agent.cli import build_agent

    agent = build_agent(str(project_root))
    glossary = json.loads(glossary_path.read_text(encoding="utf-8"))

    rows: list[list[tuple[str, bool]]] = []
    rows.append(
        [
            ("number", False),
            ("kpi name", False),
            ("calculated formula", False),
            ("type of result", False),
        ]
    )

    for idx, entry in enumerate(glossary, start=1):
        name = entry.get("name", "")
        measure = entry.get("measure", "")

        response_type = "error"
        try:
            result = agent.invoke({"question": name})
            if result.get("response"):
                payload = json.loads(result["response"])
                response_type = payload.get("type") or "error"
        except Exception:
            response_type = "error"

        rows.append(
            [
                (str(idx), True),
                (name, False),
                (measure, False),
                (response_type, False),
            ]
        )

    sheet_xml = _build_sheet_xml(rows)

    tmp = out_xlsx.with_suffix(".tmp.xlsx")
    if tmp.exists():
        tmp.unlink()

    with zipfile.ZipFile(template_xlsx, "r") as zin, zipfile.ZipFile(
        tmp, "w", compression=zipfile.ZIP_DEFLATED
    ) as zout:
        for info in zin.infolist():
            if info.filename == "xl/worksheets/sheet1.xml":
                continue
            data = zin.read(info.filename)
            zout.writestr(info, data)

        zout.writestr("xl/worksheets/sheet1.xml", sheet_xml)

        # Touch core props timestamp if present.
        try:
            core = zin.read("docProps/core.xml").decode("utf-8", errors="ignore")
            now = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            core = core.replace("</cp:lastModifiedBy>", f"</cp:lastModifiedBy>")
            # best-effort: if tags exist, replace content
            core = core.replace(
                "<dcterms:modified>1970-01-01T00:00:00Z</dcterms:modified>",
                f"<dcterms:modified>{now}</dcterms:modified>",
            )
            # If template already had core.xml, it was copied above. We won't overwrite.
        except Exception:
            pass

    tmp.replace(out_xlsx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
