import json
import os

from .agent import AiDELLM, AgentResources, create_agent
from .agent import is_junk_query
from .config import load_config
from .filters import detect_supported_relationships
from .glossary import (
    format_filter_glossary,
    format_kpi_glossary,
    load_filter_glossary,
    load_kpi_filter_map,
    load_kpi_glossary,
    load_page_defaults,
    load_report_defaults,
    load_visual_map,
)
from .tools import PowerBITool


def _apply_server_name_default_filters(
    report_defaults: list[dict],
    server_names: list[str],
) -> list[dict]:
    if not server_names:
        return report_defaults

    filtered_defaults = [
        default
        for default in report_defaults
        if default.get("name") != "ServerDetails ServerName"
    ]

    if len(server_names) == 1:
        filtered_defaults.append(
            {
                "name": "ServerDetails ServerName",
                "op": "=",
                "value": server_names[0],
            }
        )
    else:
        filtered_defaults.append(
            {
                "name": "ServerDetails ServerName",
                "op": "in",
                "value": server_names,
            }
        )
    return filtered_defaults


def build_agent(project_root: str):
    config = load_config(project_root)

    llm = AiDELLM(
        api_token=config.aide_api_token,
        base_url=config.aide_base_url,
        model=config.aide_model,
        audio_model=config.aide_audio_model,
        audio_path=config.aide_audio_path,
        realtime_path=config.aide_realtime_path,
        input_audio_format=config.aide_input_audio_format,
        output_audio_format=config.aide_output_audio_format,
        server_vad=config.aide_server_vad,
        ssl_verify=config.aide_ssl_verify,
        audio_transport=config.aide_audio_transport,
        configuration_profile=config.aide_configuration_profile,
        temperature=0,
    )
    pbi_tool = PowerBITool(
        tenant_id=config.tenant_id,
        client_id=config.client_id,
        client_secret=config.client_secret,
        dataset_id=config.dataset_id,
        workspace_id=config.workspace_id,
        impersonated_user=config.impersonated_user,
        effective_username=config.effective_username,
        effective_roles=config.effective_roles,
        rls_enabled=config.rls_enabled,
        rls_username=config.rls_username,
        rls_roles=config.rls_roles,
    )

    supported_relationships = detect_supported_relationships(pbi_tool)

    glossary_entries = load_kpi_glossary(config.glossary_path)
    glossary_text = format_kpi_glossary(glossary_entries)

    filter_entries = load_filter_glossary(config.filter_glossary_path)
    filter_text = format_filter_glossary(filter_entries)

    kpi_filter_map = load_kpi_filter_map(config.kpi_filter_map_path)
    report_defaults = load_report_defaults(config.report_defaults_path)

    if config.server_name_filter:
        manual_servers = [
            value.strip()
            for value in config.server_name_filter.split(",")
            if value and value.strip()
        ]
        report_defaults = _apply_server_name_default_filters(report_defaults, manual_servers)

    report_page_defaults = load_page_defaults(config.report_page_defaults_path)
    visual_map = load_visual_map(config.visual_map_path)

    resources = AgentResources(
        llm=llm,
        pbi_tool=pbi_tool,
        glossary_entries=glossary_entries,
        glossary_text=glossary_text,
        filter_entries=filter_entries,
        filter_text=filter_text,
        kpi_filter_map=kpi_filter_map,
        report_defaults=report_defaults,
        report_page_defaults=report_page_defaults,
        visual_map=visual_map,
        supported_relationships=supported_relationships,
    )

    return create_agent(resources)


def handle_morning_ops_check(agent):
    """Handles the Morning Operations Check flow."""
    print("\n--- Morning Operations Snapshot ---")

    kpis_to_query = [
        "Average Daily Revenue Miles",
        "Average Daily Revenue Hours",
        "AvgDailyTrips",
        "Rides Per Hour",
    ]

    for kpi in kpis_to_query:
        question = kpi
        result = agent.invoke({"question": question})
        if result.get("error"):
            print(f"Error fetching {kpi}: {result['error']}")
        else:
            try:
                response_data = json.loads(result.get("response", "{}"))
                if response_data.get("type") == "kpi":
                    title = response_data.get("title", kpi)
                    value = response_data.get("value", "N/A")
                    print(f"- {title}: {value}")
                else:
                    print(f"- {kpi}: {result.get('response', 'Could not retrieve data.')}")
            except json.JSONDecodeError:
                print(f"- {kpi}: {result.get('response', 'Could not retrieve data.')}")

    drill_down = input("\nWould you like to see top performing vehicles? (yes/no) > ")
    if drill_down.lower() == "yes":
        top_n_queries = [
            "Top 5 vehicles by revenue miles",
            "Top 5 vehicles by revenue hours",
        ]
        for query in top_n_queries:
            result = agent.invoke({"question": query})
            if result.get("error"):
                print(f"Error fetching {query}: {result['error']}")
            else:
                try:
                    response_data = json.loads(result.get("response", "{}"))
                    if response_data.get("type") == "topn":
                        print(f"\n--- {response_data.get('title', query)} ---")
                        for row in response_data.get("rows", []):
                            print(f"- {row[0]}: {row[1]}")
                    else:
                        print(
                            f"\nResult for '{query}':\n{result.get('response', 'Could not retrieve data.')}"
                        )
                except json.JSONDecodeError:
                    print(
                        f"\nResult for '{query}':\n{result.get('response', 'Could not retrieve data.')}"
                    )


def handle_otp_investigation(agent):
    """Handles the OTP Investigation flow."""
    print("\n--- OTP Investigation ---")

    otp_kpis = ["Pickup On-Time Performance", "Dropoff On-Time Performance"]

    for otp_kpi in otp_kpis:
        question = otp_kpi
        result = agent.invoke({"question": question})

        if result.get("error"):
            print(f"Error fetching {otp_kpi}: {result['error']}")
            continue

        try:
            response_data = json.loads(result.get("response", "{}"))
            if response_data.get("type") == "kpi":
                title = response_data.get("title", otp_kpi)
                value = response_data.get("value", "N/A")
                print(f"{title}: {value}%")
            else:
                print(f"{otp_kpi}: {result.get('response', 'Could not retrieve data.')}")
        except json.JSONDecodeError:
            print(f"{otp_kpi}: {result.get('response', 'Could not retrieve data.')}")

    while True:
        print("\nHow would you like to investigate further?")
        print("1. View OTP by Driver")
        print("2. View OTP by Vehicle")
        print("3. Show late trip details")
        print("4. Back to main menu")
        choice = input("> ")

        if choice == "4":
            break

        if choice in ["1", "2"]:
            dimension = "Driver" if choice == "1" else "Vehicle"
            print(f"\n--- OTP by {dimension} ---")

            if dimension == "Driver":
                query = "Pickup OTP and Dropoff OTP by Driver"
            else:
                query = "Pickup OTP and Dropoff OTP by Vehicle"

            result = agent.invoke({"question": query})
            if result.get("error"):
                print(f"Error fetching data: {result['error']}")
            else:
                try:
                    response_data = json.loads(result.get("response", "{}"))
                    response_type = response_data.get("type")
                    if response_type == "bar":
                        rows = response_data.get("rows", [])
                        for label, value in rows:
                            print(f"{label}: {value}")
                    else:
                        print(result.get("response", "Could not retrieve data."))
                except json.JSONDecodeError:
                    print(result.get("response", "Could not retrieve data."))

        elif choice == "3":
            donut_query = "Donut chart of [Total Trips] grouped by the 'ArchiveTrip'[pickup_ontime]"
            donut_result = agent.invoke({"question": donut_query})
            if donut_result.get("error"):
                print(f"Error fetching pickup OTP donut chart: {donut_result['error']}")
            else:
                try:
                    donut_data = json.loads(donut_result.get("response", "{}"))
                    if donut_data.get("type") == "pie":
                        print("\n--- Trips Pickup OTP Donut Chart ---")
                        for label, value in donut_data.get("rows", []):
                            print(f"- {label}: {value}")
                    else:
                        print(donut_result.get("response", "Could not retrieve donut chart."))
                except json.JSONDecodeError:
                    print(donut_result.get("response", "Could not retrieve donut chart."))

            query = "Show details for trips with late pickups"
            result = agent.invoke({"question": query})
            if result.get("error"):
                print(f"Error fetching data: {result['error']}")
            else:
                try:
                    response_data = json.loads(result.get("response", "{}"))
                    response_type = response_data.get("type")
                    print(f"\n--- {response_data.get('title', query)} ---")
                    if response_type == "table":
                        rows = response_data.get("rows", [])
                        if rows:
                            headers = list(rows[0].keys())
                            col_widths = {
                                h: max(len(h), max(len(str(r.get(h, ""))) for r in rows))
                                for h in headers
                            }
                            header_line = " | ".join(
                                f"{h:<{col_widths[h]}}" for h in headers
                            )
                            print(header_line)
                            print("-" * len(header_line))
                            for row in rows:
                                row_line = " | ".join(
                                    f"{str(row.get(h, '')):<{col_widths[h]}}"
                                    for h in headers
                                )
                                print(row_line)
                        else:
                            print("No details found for late trips.")
                    else:
                        print(result.get("response", "Could not retrieve data."))
                except json.JSONDecodeError:
                    print(result.get("response", "Could not retrieve data."))
        else:
            print("Invalid choice. Please try again.")


def main():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    agent = build_agent(project_root)

    print("Welcome to the Power BI LangGraph Agent (powered by AiDE LLM)!")
    print("Select a flow or ask a question about your KPIs (or type 'exit' to quit).")

    while True:
        print("\nAvailable Flows:")
        print("1. Morning Operations Check")
        print("2. OTP Investigation")
        user_input = input("> ")
        print(user_input)

        if is_junk_query(user_input):
          print("Hello! I'm your Power BI assistant. How can I help you with your KPIs today?")
          continue  # Skip sending this to build_query entirely

        if user_input.lower() == "morning operations check":
            handle_morning_ops_check(agent)
        elif user_input.lower() == "otp investigation":
            handle_otp_investigation(agent)
        elif user_input.lower() == "exit":
            break
        else:
            result = agent.invoke({"question": user_input})
            if result.get("error"):
                print(f"An error occurred: {result['error']}")
            else:
                print(result.get("response", "No response returned."))
        
