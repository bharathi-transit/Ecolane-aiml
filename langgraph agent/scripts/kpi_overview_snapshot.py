import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    os.chdir(project_root)
    sys.path.insert(0, str(project_root))

    from src.pbi_agent.cli import build_agent

    agent = build_agent(str(project_root))

    # Mirrors the KPI Overview page visuals.
    queries = [
        "Average Daily Revenue Miles",
        "Average Daily Revenue Hours",
        "Average Daily Trips",
        "Rides Per Hour",
        "Pickup On-Time Performance",
        "Service vs Revenue Miles",
        "Service vs Revenue Hours",
        "Passenger Trips status",
        "Rides per hour",
        "Pickup OTP",
        "Top 5 vehicles by revenue miles",
        "Top 5 vehicles by revenue hours",
        "Top 5 vehicles by completed trips",
        "Top 5 vehicles by rides per hour",
        "Top 5 vehicles by otp%",
    ]

    results = []
    for q in queries:
        r = agent.invoke({"question": q})
        entry = {"question": q}
        if r.get("error"):
            entry["error"] = r["error"]
        else:
            entry["response"] = r.get("response", "")
        results.append(entry)

    out_dir = project_root / "reports"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"kpi_overview_snapshot_{ts}.json"
    out_path.write_text(json.dumps({"generated_utc": ts, "results": results}, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

