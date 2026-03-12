import json
import base64
import json
import ssl
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from urllib.parse import urljoin
import re

import websocket

import requests
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.graph import END, StateGraph

import re

import re

def is_junk_query(query: str) -> bool:
    """
    Uses pattern matching to catch small talk, typos, and fillers.
    """
    if not query or not query.strip():
        return True
        
    # Lowercase and remove punctuation at the ends
    clean_query = query.lower().strip().rstrip('?!.')
    
    # Define flexible patterns. 
    # The '.*' acts as a wildcard. So 'how.*you' catches:
    # "how are you", "how re you", "how r u", "how do you do"
    junk_patterns = [
        r"^(hi|hello|hey|hiya|yo)\b",               # Greetings
        r"^how are you\b",                          # How are you
        r"^what'?s up\b",                           # Catches "what's up" and "whats up"
        r"^good\s+(morning|afternoon|evening)",     # Time-based greetings
        r"^(thanks|thank you|ok|okay|test)\b"       # Closers/Fillers
    ]
    
    # 1. Check against patterns
    for pattern in junk_patterns:
        if re.search(pattern, clean_query):
            return True
            
    # 2. Minimum Length Guard
    # Strip all spaces/special chars. If the remaining text is less than 
    # 3 letters long (e.g., "a", "ok", "??"), it's junk.
    alpha_num_only = re.sub(r'[^a-z0-9]', '', clean_query)
    if len(alpha_num_only) < 3:
        return True
        
    return False

from .dax import (
    build_detail_dax,
    build_filtered_dax,
    build_grouped_dax,
    build_kpi_dax,
    build_multi_measure_dax,
    build_topn_dax,
)
from .filters import (
    apply_date_relationship,
    build_filter_expression,
    is_all_value,
    is_archive_status_filter,
    resolve_filter_name,
    select_date_relationship,
    select_primary_date_filter,
)
from .glossary import iter_kpi_aliases, match_kpi_from_question, measure_name_from_expression
from .models import AgentState, FilterSpec, KPIQuery
from .parsing import (
    detect_chart_template,
    detect_comparison_chart_pattern,
    detect_comparison_query,
    detect_detail_template,
    detect_page,
    infer_query_type_from_text,
    is_explicit_topn_question,
    parse_explicit_grouped_query,
    parse_relative_date_range,
)
from .text_utils import normalize_text
from .tools import PowerBITool
from .utils import extract_kpi_value, float_safe, format_number


class AiDELLM:
    def __init__(
        self,
        api_token: str,
        base_url: str = "https://aide.transit-technologies.ai/api/v1/brownfield/",
        model: str = "gpt-4o-mini",
        audio_model: str = "gpt-realtime-mini",
        audio_path: str = "completions",
        realtime_path: str = "realtime",
        input_audio_format: str = "pcm16",
        output_audio_format: str = "opus",
        server_vad: bool = False,
        audio_transport: str = "http",  # "http" (completions) or "realtime" (websocket)
        ssl_verify: bool | str = True,
        configuration_profile: str | None = None,
        temperature: float = 0,
    ):
        self.api_token = api_token
        # Normalize so urljoin appends correctly.
        self.base_url = base_url.rstrip("/") + "/"
        self.model = model
        self.audio_model = audio_model
        # Allow overriding the path in case the deployment exposes a different audio route.
        self.audio_path = (audio_path or "completions").strip("/")
        self.realtime_path = realtime_path.strip("/") or "realtime"
        self.input_audio_format = input_audio_format
        self.output_audio_format = output_audio_format
        self.server_vad = server_vad
        self.configuration_profile = configuration_profile
        self.temperature = temperature
        self.ssl_verify = ssl_verify
        self.audio_transport = (audio_transport or "http").lower()

    def _headers(self) -> dict:
        headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }
        if self.configuration_profile:
            headers["X-Configuration-Profile"] = self.configuration_profile
        return headers

    def invoke(self, prompt: str) -> str:
        """Text-only completion (backwards compatible)."""
        return self.invoke_text(prompt)

    def invoke_text(self, prompt: str) -> str:
        url = "https://aide.transit-technologies.ai/api/v1/brownfield/completions"

        messages = []
        # The prompt contains both system instructions and the user question.
        # We need to separate them to fit the 'messages' format.
        try:
            question_marker = "Question: "
            question_start_index = prompt.find(question_marker)
            
            # The system prompt is everything before "Question: "
            system_content = prompt[:question_start_index].strip()

            # The user's question is the part after the marker. It ends before the response instructions.
            response_marker = "\n\nResponse (JSON format):"
            question_end_index = prompt.find(response_marker, question_start_index)
            
            user_content = prompt[question_start_index + len(question_marker) : question_end_index].strip()
            
            messages.append({"role": "system", "content": system_content})
            messages.append({"role": "user", "content": user_content})
        except Exception:
            # Fallback if the prompt format is unexpected, which can happen with generic queries.
            messages.append({"role": "user", "content": prompt})

        payload = {
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": 5120,
            "stream": False,
        }

        response = requests.post(
            url, headers=self._headers(), json=payload, timeout=60, verify=self.ssl_verify
        )

        print(f"AiDE Response: {response.status_code} {response.text}")
        
        # A 204 status means success but no content. We should not proceed to parse JSON.
        if response.status_code == 204:
            return ""

        response.raise_for_status()

        try:
            data = response.json()
        except (ValueError, json.JSONDecodeError):
            # If the response isn't JSON, return the raw text.
            return response.text.strip()

        # Handle standard OpenAI-like response structure.
        if "choices" in data and data.get("choices"):
            first_choice = data["choices"][0]
            if "message" in first_choice and "content" in first_choice["message"]:
                return first_choice["message"]["content"] or ""
            if "text" in first_choice:
                return first_choice["text"] or ""

        # Fallback to a custom 'output' field some models use.
        if "output" in data:
            return data.get("output", "")

        return ""

    def _build_ws_url(self, path: str) -> str:
        """Convert the base HTTP(S) endpoint to WS(S)."""
        http_url = urljoin(self.base_url, path)
        if http_url.startswith("https://"):
            return "wss://" + http_url[len("https://") :]
        if http_url.startswith("http://"):
            return "ws://" + http_url[len("http://") :]
        return http_url

    def invoke_audio(
        self,
        audio_bytes: bytes,
        mime_type: str = "audio/wav",
        input_audio_format: str | None = None,
        prompt: str | None = None,
        max_tokens: int = 500,
        temperature: float | None = None,
    ) -> str:
        """
        Sends raw audio to AiDE using the gpt-realtime-mini model and returns text.

        - audio_bytes: raw PCM/encoded bytes from the frontend.
        - mime_type: e.g., audio/wav, audio/mpeg, audio/webm.
        - prompt: optional system/user priming text alongside audio.
        """
        url = urljoin(self.base_url, self.audio_path)
        b64_audio = base64.b64encode(audio_bytes).decode("ascii")
        audio_format = (input_audio_format or mime_type.split("/")[-1])
        payload = {
            "model": self.audio_model,
            "prompt": prompt or "",
            # Some AiDE deployments expect a top-level 'audio' field; include it.
            "audio": b64_audio,
            "input_audio": {
                "data": b64_audio,
                "format": audio_format,
            },
            "input_audio_format": audio_format,
            "max_tokens": max_tokens,
            "temperature": temperature if temperature is not None else self.temperature,
        }

        response = requests.post(
            url, headers=self._headers(), json=payload, timeout=60, verify=self.ssl_verify
        )
        response.raise_for_status()
        try:
            data = response.json()
        except ValueError:
            # Non-JSON payload (e.g., empty body or text); return raw text.
            if response.text:
                return response.text
            return f"[http {response.status_code}] Empty body from AiDE audio endpoint"

        # Try common response shapes.
        if isinstance(data, dict):
            if "text" in data:
                return data["text"]
            if "transcript" in data:
                return data["transcript"]
            choices = data.get("choices")
            if choices and isinstance(choices, list):
                return choices[0].get("text") or choices[0].get("message") or ""
        return str(data)

    def invoke_audio_realtime(
        self,
        audio_bytes: bytes,
        input_format: str | None = None,
        output_format: str | None = None,
        prompt: str | None = None,
        timeout: float = 20.0,
    ) -> str:
        """
        Sends raw audio over AiDE's realtime WebSocket and returns aggregated text.
        """
        ws_url = self._build_ws_url(self.realtime_path)
        headers = [
            f"Authorization: Bearer {self.api_token}",
        ]
        if self.configuration_profile:
            headers.append(f"X-Configuration-Profile: {self.configuration_profile}")

        sslopt = None
        if self.ssl_verify is False:
            sslopt = {"cert_reqs": ssl.CERT_NONE}
        elif isinstance(self.ssl_verify, str):
            sslopt = {"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": self.ssl_verify}

        ws = websocket.create_connection(ws_url, header=headers, timeout=timeout, sslopt=sslopt)
        try:
            session_msg = {
                "type": "session.update",
                "session": {
                    "model": self.audio_model,
                    "input_audio_format": input_format or self.input_audio_format,
                    "output_audio_format": output_format or self.output_audio_format,
                },
            }
            if prompt:
                session_msg["session"]["instructions"] = prompt
            # Optional server VAD flag if supported by backend.
            if self.server_vad:
                session_msg["session"]["input_audio_vad"] = {"enabled": True}

            ws.send(json.dumps(session_msg))

            append_msg = {
                "type": "input_audio_buffer.append",
                "audio": base64.b64encode(audio_bytes).decode("ascii"),
            }
            ws.send(json.dumps(append_msg))

            commit_msg = {"type": "input_audio_buffer.commit"}
            ws.send(json.dumps(commit_msg))

            text_chunks: list[str] = []
            start = time.time()
            while time.time() - start < timeout:
                raw = ws.recv()
                if not raw:
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue

                event_type = event.get("type")
                if event_type == "response.output_text.delta":
                    delta = event.get("delta", {})
                    content = delta.get("content") or delta.get("text") or ""
                    if content:
                        text_chunks.append(content)
                elif event_type in {
                    "response.completed",
                    "response.end",
                    "response.done",
                }:
                    break
            return "".join(text_chunks).strip()
        finally:
            try:
                ws.close()
            except Exception:
                pass


def parse_llm_response(response_text: str) -> KPIQuery:
    if not response_text or not response_text.strip():
        return KPIQuery(kpi_name="UNKNOWN")

    # Use regex to find JSON content within ```json ... ``` or ``` ... ```
    match = re.search(r"\{.*\}", response_text, re.DOTALL)
    if not match:
        print(f"Warning: No JSON object found in LLM response. Response: '{response_text}'")
        return KPIQuery(kpi_name="UNKNOWN")
    
    cleaned_text = match.group(0)

    try:
        data = json.loads(cleaned_text)

        filters = data.get("filters", [])
        if isinstance(filters, dict):
            filters = [FilterSpec(**f) for f in filters.values()] if filters else []
        elif isinstance(filters, list):
            filters = [FilterSpec(**f) for f in filters]

        return KPIQuery(
            kpi_name=data.get("kpi_name", "UNKNOWN"),
            filters=filters,
            query_type=data.get("query_type", "kpi"),
            group_by=data.get("group_by"),
            limit=data.get("limit") if data.get("limit") is not None else 5,
            order=data.get("order", "DESC"),
            table=data.get("table"),
            columns=data.get("columns", []),
        )
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        print(
            f"Warning: Could not parse LLM response due to: {exc}. Response: '{response_text}'"
        )
        return KPIQuery(kpi_name="UNKNOWN")


@dataclass
class AgentResources:
    llm: AiDELLM
    pbi_tool: PowerBITool
    glossary_entries: list[dict]
    glossary_text: str
    filter_entries: list[dict]
    filter_text: str
    kpi_filter_map: dict
    report_defaults: list[dict]
    report_page_defaults: dict
    visual_map: dict
    supported_relationships: dict[str, str]


PROMPT_TEMPLATE = """
You are a Power BI analyst. Use the KPI glossary to map the user's question to a KPI.
Only use measures explicitly listed in the glossary. If you cannot match a KPI, respond
with kpi_name = "UNKNOWN" and no filters.

KPI glossary:
{glossary}

Filter glossary (use ONLY these filter names):
{filters}

Filter rules:
- Use filter names from the glossary exactly.
- Use operators: "=", "!=", "in", "between", ">=", "<=".
- Dates must be formatted as YYYY-MM-DD strings.
- For "between", use a list of two values.
- For "in", use a list of values.
- If no filter is requested, return an empty filters list.

If the user asks for a ranked list (e.g., "top 5 vehicles by revenue miles"),
set query_type = "topn", set group_by to a filter name (dimension),
set limit to the requested number, and set order to "DESC".
If the user asks for a bottom list (e.g., "bottom 5"), set order to "ASC".
If the user asks for a bar chart or pie chart, set query_type = "barchart" or "pie"
and provide group_by and limit (optional).
If the user asks for details or a table, set query_type = "detail" and provide a table
name and columns (use filter glossary names).
Otherwise set query_type = "kpi".

You must respond with a JSON object that follows this schema:
{format_instructions}

Question: {question}

Response (JSON format):
"""


def build_prompt(parser: PydanticOutputParser) -> PromptTemplate:
    return PromptTemplate(
        template=PROMPT_TEMPLATE,
        input_variables=["question", "glossary", "filters"],
        partial_variables={"format_instructions": parser.get_format_instructions()},
    )


def _is_exact_kpi_request(question: str, kpi_entry: dict | None) -> bool:
    if not question or not kpi_entry:
        return False
    normalized_question = normalize_text(question)
    if not normalized_question:
        return False
    for alias in iter_kpi_aliases(kpi_entry):
        if normalize_text(alias) == normalized_question:
            return True
    return False


def create_agent(resources: AgentResources):
    parser = PydanticOutputParser(pydantic_object=KPIQuery)
    prompt = build_prompt(parser)

    def default_to_filterspec(default: dict) -> FilterSpec | None:
        name = default.get("name", "")
        if not name:
            return None

        op = (default.get("op", "=") or "=").strip().lower()
        value = default.get("value")

        # Supports dynamic rolling windows from config defaults.
        if op in {"rolling_days", "last_n_days"}:
            try:
                days = int(value)
            except (TypeError, ValueError):
                days = 120
            days = max(1, days)
            end_date = date.today()
            start_date = end_date - timedelta(days=days - 1)
            return FilterSpec(
                name=name,
                op="between",
                value=[start_date.isoformat(), end_date.isoformat()],
            )

        return FilterSpec(name=name, op=default.get("op", "="), value=value)

    def build_query(state: AgentState) -> dict:
        try:
            # LLM-first approach: Always try to interpret the full question first.
            prompt_text = prompt.format(
                question=state["question"],
                glossary=resources.glossary_text,
                filters=resources.filter_text,
            )
            response = resources.llm.invoke(prompt_text)
            llm_result = parse_llm_response(response)

            # If the LLM successfully identified a KPI, trust it.
            if llm_result and llm_result.kpi_name != "UNKNOWN":
                inferred_type = infer_query_type_from_text(state["question"])
                return {
                    "kpi_name": llm_result.kpi_name,
                    "filters": llm_result.filters,
                    "query_type": inferred_type or llm_result.query_type or "kpi",
                    "group_by": llm_result.group_by,
                    "limit": llm_result.limit or 5,
                    "order": llm_result.order or "DESC",
                    "table": llm_result.table,
                    "columns": llm_result.columns,
                    "skip_defaults": False,
                }

            # --- Fallback Logic ---
            # If the LLM failed, now we try simpler, direct matching methods.
            direct_match = match_kpi_from_question(
                state.get("question", ""), resources.glossary_entries
            )
            if direct_match:
                inferred_type = infer_query_type_from_text(state["question"])
                # Only return a simple KPI if that's what's inferred.
                if not inferred_type or inferred_type == "kpi":
                    return {
                        "kpi_name": direct_match.get("name", ""),
                        "filters": [],
                        "query_type": "kpi",
                        "group_by": None,
                        "limit": 5,
                        "order": "DESC",
                        "table": None,
                        "columns": [],
                        "skip_defaults": False,
                    }

            # Final fallback if both LLM and direct match fail.
            print("[DEBUG] Fallback triggered: LLM and direct match failed.")
            return {
                "fallback": True,
                "response": "I'm sorry, I could not understand the KPI in your request. Please try rephrasing your question."
            }

        except Exception as exc:
            print(f"[ERROR] build_query failed: {exc}")
            return {
                "fallback": True,
                "response": "I encountered an unexpected error while trying to understand your request. Please try again."
            }

    def run_query(state: AgentState) -> dict:
        if state.get("response") and not state.get("query_type"):
            return {}

       # if state.get("error"):
           # return {}

        print(
            f"[DEBUG] run_query - query_type: {state.get('query_type')}, has comparison_measures: {bool(state.get('comparison_measures'))}"
        )
        if state.get("query_type") == "comparison_chart" and state.get(
            "comparison_measures"
        ):
            print("[DEBUG] Entering comparison_chart handler")
            group_by_name = state.get("group_by")
            if not group_by_name:
                return {
                    "response": "Comparison chart requires a grouping dimension (e.g., Date)."
                }

            group_by_entry = resolve_filter_name(group_by_name, resources.filter_entries)
            if not group_by_entry:
                return {
                    "response": f"Could not resolve grouping dimension: {group_by_name}"
                }

            measures = []
            for kpi_entry in state.get("comparison_measures", []):
                kpi_name = kpi_entry.get("name", "")
                relationship = select_date_relationship(
                    state.get("question", ""), kpi_entry
                )
                date_column = (
                    resources.supported_relationships.get(relationship)
                    if relationship
                    else None
                )
                measure_expr = apply_date_relationship(
                    kpi_entry.get("measure", ""),
                    relationship if date_column else None,
                    date_column,
                )
                if measure_expr:
                    measures.append((kpi_name, measure_expr))

            if len(measures) < 2:
                return {"response": "Comparison chart requires at least 2 measures."}

            filter_expressions = []
            dimension_filters = set()
            for f_entry in resources.filter_entries:
                f_type = (f_entry.get("type") or "").lower()
                if f_type in ["text"]:
                    dimension_filters.add(f_entry.get("name", ""))

            critical_dimension_defaults = {
                "ArchiveTrip status",
                "ArchiveTrip Friendly_Status",
                "ArchiveTrip pickup_ontime",
                "ArchiveTrip dropoff_ontime",
                "ArchiveTrip cancel_type",
                "ArchiveTrip cancel_type_display_text",
                "ArchiveTrip Agent",
                "ServerDetails Agent",
                "ServerDetails ServerName",
                "Date_time Day Name",
                "Valid Distance in meters",
                "Trip Types Trip Types",
                "NoshowFilter Include Noshows",
                "IncludeRuns Include Runs",
            }

            merged_filters = []
            if not state.get("skip_defaults"):
                for default in resources.report_defaults:
                    default_name = default.get("name", "")
                    entry = resolve_filter_name(default_name, resources.filter_entries)
                    if not entry:
                        continue
                    if (
                        entry.get("name") in dimension_filters
                        and entry.get("name") not in critical_dimension_defaults
                    ):
                        continue
                    if is_archive_status_filter(entry):
                        value = default.get("value")
                        if isinstance(value, list):
                            cleaned = [v for v in value if not is_all_value(v)]
                            if len(cleaned) > 1:
                                continue
                    default_spec = default_to_filterspec(default)
                    if default_spec:
                        merged_filters.append(default_spec)

            for spec in merged_filters:
                entry = resolve_filter_name(spec.name, resources.filter_entries)
                if entry:
                    filter_expressions.extend(build_filter_expression(entry, spec))

            dax_query = build_multi_measure_dax(
                measures,
                group_by_entry.get("column", ""),
                filter_expressions,
                limit=state.get("limit", 100),
                order=state.get("order", "ASC"),
            )

            if not dax_query:
                return {"response": "Could not build comparison chart query."}

            try:
                result = resources.pbi_tool.execute_query(dax_query)
                return {
                    "query_result": result,
                    "query_type": "comparison_chart",
                    "chart_type": state.get("chart_type", "line"),
                    "comparison_measures": [m[0] for m in measures],
                    "group_by": group_by_name,
                }
            except Exception as exc:
                return {"error": f"Error executing comparison chart query: {exc}"}

        if state.get("query_type") == "comparison" and state.get("comparison_kpis"):
            comparison_results = []
            for kpi_entry in state.get("comparison_kpis", []):
                kpi_name = kpi_entry.get("name", "")
                measure = kpi_entry.get("measure", "")
                if not measure:
                    continue

                relationship = select_date_relationship(
                    state.get("question", ""), kpi_entry
                )
                measure_expr = apply_date_relationship(measure, relationship)
                dax_query = build_kpi_dax(measure_expr)
                if not dax_query:
                    continue

                try:
                    result = resources.pbi_tool.execute_query(dax_query)
                    kpi_value = extract_kpi_value(result)
                    comparison_results.append(
                        {
                            "name": kpi_name,
                            "value": format_number(kpi_value) if kpi_value else "N/A",
                        }
                    )
                except Exception as exc:
                    comparison_results.append({"name": kpi_name, "value": f"Error: {exc}"})

            if comparison_results:
                payload = {
                    "type": "comparison",
                    "title": "Comparison Results",
                    "comparisons": comparison_results,
                }
                return {"response": json.dumps(payload, ensure_ascii=True)}
            return {"response": "Could not retrieve comparison data."}

        chart_template = detect_chart_template(state.get("question", ""), resources.visual_map)
        detail_template = detect_detail_template(state.get("question", ""), resources.visual_map)

        query_type = (state.get("query_type") or "kpi").lower()
        if chart_template:
            template_qt = (chart_template.get("query_type") or "barchart").lower()
            if (
                template_qt in {"barchart", "pie"}
                and query_type not in {"detail", "kpi"}
                and not is_explicit_topn_question(state.get("question", ""))
            ):
                query_type = template_qt
                state["query_type"] = query_type

        if not state.get("kpi_name") and not chart_template and not detail_template:
            return {
                "response": (
                    "I could not determine the KPI to query. "
                    "Please rephrase or include the KPI name explicitly."
                )
            }

        kpi_name = state.get("kpi_name")
        if (
            chart_template
            and chart_template.get("kpi_name")
            and (state.get("query_type") or "").lower() not in {"detail", "kpi"}
        ):
            kpi_name = chart_template.get("kpi_name")

        kpi_entry = (
            match_kpi_from_question(str(kpi_name), resources.glossary_entries)
            if kpi_name
            else None
        )
        if not kpi_entry and query_type == "detail" and detail_template:
            kpi_name = detail_template.get("__name__") or kpi_name or "Detail"
            kpi_entry = {"name": kpi_name, "measure": ""}
        if not kpi_entry:
            return {
                "response": (
                    "I could not determine the KPI to query. "
                    "Please rephrase or include the KPI name explicitly."
                )
            }

        relationship = select_date_relationship(state.get("question", ""), kpi_entry)
        date_column = (
            resources.supported_relationships.get(relationship) if relationship else None
        )
        measure_expr = apply_date_relationship(
            kpi_entry.get("measure", ""),
            relationship if date_column else None,
            date_column,
        )

        allowed_filters = set()
        kpi_key = kpi_entry.get("name", "")
        measure_key = measure_name_from_expression(kpi_entry.get("measure", ""))
        allowed_config = (
            resources.kpi_filter_map.get(kpi_key, {})
            or resources.kpi_filter_map.get(measure_key, {})
        )
        if isinstance(allowed_config, dict):
            allowed_filters = set(allowed_config.get("allowed_filters", []) or [])
            if "Date_time L_Date" in allowed_filters:
                allowed_filters.add("Date_time Date_Key")

        dimension_filters = set()
        for f_entry in resources.filter_entries:
            f_type = (f_entry.get("type") or "").lower()
            f_name = f_entry.get("name", "")
            if f_type in ["text"]:
                dimension_filters.add(f_name)
            elif f_type == "number" and any(
                keyword in f_name.lower()
                for keyword in ["year", "month", "quarter", "day of week", "hour"]
            ):
                dimension_filters.add(f_name)

        query_type = (state.get("query_type") or "kpi").lower()
        if chart_template:
            template_qt = (chart_template.get("query_type") or "barchart").lower()
            if (
                template_qt in {"barchart", "pie"}
                and query_type not in {"detail", "kpi"}
                and not is_explicit_topn_question(state.get("question", ""))
            ):
                query_type = template_qt
                state["query_type"] = query_type
        is_grouped_query = query_type in {"topn", "barchart", "pie"}
        group_by_table = None
        group_by_name = None
        if is_grouped_query and state.get("group_by"):
            gb_entry = resolve_filter_name(
                state.get("group_by", ""), resources.filter_entries
            )
            if gb_entry and gb_entry.get("column"):
                group_by_name = gb_entry.get("name")
                group_by_table = gb_entry["column"].split("[", 1)[0].strip().strip("'")

        filter_expressions: list[str] = []
        rejected: list[str] = []

        merged_filters: list[FilterSpec] = []
        seen = set()
        question_text = state.get("question", "") or ""
        page_name = detect_page(question_text, resources.report_page_defaults)

        kpi_overview_kpis = {
            "Average Daily Revenue Miles",
            "Average Daily Revenue Hours",
            "Average Daily Trips",
            "Average Daily Completed Passenger Trips",
            "Rides Per Hour",
            "Pickup On-Time Performance",
            "Total Revenue Miles",
            "Total Revenue Hours",
            "Completed Trips",
        }
        kpi_overview_templates = {
            "passenger trips status",
            "rides per hour",
            "pickup otp",
            "top 5 vehicles by revenue miles",
            "top 5 vehicles by revenue hours",
            "top 5 vehicles by completed trips",
            "top 5 vehicles by rides per hour",
            "top 5 vehicles by otp%",
        }
        # If the user is asking for a KPI Overview tile/visual, don't let generic page
        # keyword matching (e.g., "trips") apply the wrong page-level defaults.
        if (kpi_entry or {}).get("name") in kpi_overview_kpis and _is_exact_kpi_request(
            question_text, kpi_entry
        ):
            page_name = "KPI Overview"
        else:
            template = detect_chart_template(question_text, resources.visual_map) or {}
            if normalize_text(str(template.get("__name__", ""))) in kpi_overview_templates:
                page_name = "KPI Overview"

        for spec in state.get("filters", []) or []:
            entry = resolve_filter_name(spec.name, resources.filter_entries)
            if entry:
                seen.add(entry.get("name"))
            merged_filters.append(spec)

        critical_dimension_defaults = {
            "ArchiveTrip status",
            "ArchiveTrip Friendly_Status",
            "ArchiveTrip pickup_ontime",
            "ArchiveTrip dropoff_ontime",
            "ArchiveTrip cancel_type",
            "ArchiveTrip cancel_type_display_text",
            "ArchiveTrip Agent",
            "ServerDetails Agent",
            "ServerDetails ServerName",
            "Date_time Day Name",
            "Valid Distance in meters",
            "Trip Types Trip Types",
            "NoshowFilter Include Noshows",
            "IncludeRuns Include Runs",
        }

        if not state.get("skip_defaults"):
            for default in resources.report_defaults:
                default_name = default.get("name", "")
                entry = resolve_filter_name(default_name, resources.filter_entries)
                if not entry or entry.get("name") in seen:
                    continue

                if (
                    is_grouped_query
                    and entry.get("name") in dimension_filters
                    and entry.get("name") not in critical_dimension_defaults
                ):
                    continue
                if is_grouped_query and is_archive_status_filter(entry):
                    value = default.get("value")
                    if isinstance(value, list):
                        cleaned = [v for v in value if not is_all_value(v)]
                        if len(cleaned) > 1:
                            continue

                default_spec = default_to_filterspec(default)
                if default_spec:
                    merged_filters.append(default_spec)

        if (
            not state.get("skip_defaults")
            and page_name
            and page_name in resources.report_page_defaults
        ):
            for default in resources.report_page_defaults[page_name]:
                default_name = default.get("name", "")
                entry = resolve_filter_name(default_name, resources.filter_entries)
                if not entry or entry.get("name") in seen:
                    continue

                if (
                    is_grouped_query
                    and entry.get("name") in dimension_filters
                    and entry.get("name") not in critical_dimension_defaults
                ):
                    continue
                if is_grouped_query and is_archive_status_filter(entry):
                    value = default.get("value")
                    if isinstance(value, list):
                        cleaned = [v for v in value if not is_all_value(v)]
                        if len(cleaned) > 1:
                            continue

                default_spec = default_to_filterspec(default)
                if default_spec:
                    merged_filters.append(default_spec)

        for spec in merged_filters:
            entry = resolve_filter_name(spec.name, resources.filter_entries)
            effective_spec = spec
            if (
                group_by_table == "ArchiveTrip"
                and entry
                and entry.get("name") in {"Date_time Date_Key", "Date_time L_Date"}
            ):
                alt_entry = resolve_filter_name(
                    "ArchiveTrip reporting_date", resources.filter_entries
                )
                if alt_entry:
                    entry = alt_entry
                    effective_spec = FilterSpec(
                        name=alt_entry.get("name", ""),
                        op=spec.op,
                        value=spec.value,
                    )
            if not entry:
                rejected.append(spec.name)
                continue
            if (
                entry.get("name") == "ArchiveTrip cancel_type"
                and (kpi_entry or {}).get("name") in {"Total Cancellations", "Same Day Cancels"}
                and group_by_name not in {"ArchiveTrip cancel_type", "ArchiveTrip cancel_type_display_text"}
            ):
                continue
            if (
                allowed_filters
                and entry.get("name") not in allowed_filters
                and entry.get("name") not in critical_dimension_defaults
            ):
                rejected.append(entry.get("name", spec.name))
                continue
            filter_expressions.extend(build_filter_expression(entry, effective_spec))

        if not any(
            resolve_filter_name(spec.name, resources.filter_entries)
            and (resolve_filter_name(spec.name, resources.filter_entries) or {}).get("type")
            == "date"
            for spec in state.get("filters", []) or []
        ):
            relative = parse_relative_date_range(state.get("question", ""), date.today())
            if relative:
                primary_date = select_primary_date_filter(resources.filter_entries)
                if primary_date and (
                    not allowed_filters or primary_date.get("name") in allowed_filters
                ):
                    spec = FilterSpec(
                        name=primary_date.get("name", ""),
                        op="between",
                        value=list(relative),
                    )
                    filter_expressions.extend(build_filter_expression(primary_date, spec))

        query_type = (state.get("query_type") or "kpi").lower()
        if chart_template and query_type in {"barchart", "pie", "topn"}:
            if not state.get("group_by") and chart_template.get("group_by"):
                state["group_by"] = chart_template.get("group_by")
            if not state.get("limit") and chart_template.get("limit"):
                state["limit"] = chart_template.get("limit")

        template_group_by = (chart_template or {}).get("group_by")
        using_template_group_by = bool(
            template_group_by
            and normalize_text(str(state.get("group_by", "")))
            == normalize_text(str(template_group_by))
        )

        if query_type == "topn":
            group_by_entry = None
            if state.get("group_by"):
                group_by_entry = resolve_filter_name(
                    state.get("group_by", ""), resources.filter_entries
                )
            if not group_by_entry:
                return {
                    "response": (
                        "I could not determine the group-by field for the ranked list. "
                        "Please rephrase and include the field name."
                    )
                }
            if (
                allowed_filters
                and group_by_entry.get("name") not in allowed_filters
                and not using_template_group_by
            ):
                return {
                    "response": (
                        "The requested group-by field is not valid for this KPI. "
                        "Please choose a different field."
                    )
                }

            topn_filter_expressions = []
            group_by_name = group_by_entry.get("name", "")

            for spec in merged_filters:
                entry = resolve_filter_name(spec.name, resources.filter_entries)
                if not entry:
                    continue

                entry_name = entry.get("name")

                if (
                    entry_name in dimension_filters
                    and entry_name != group_by_name
                    and entry_name not in critical_dimension_defaults
                ):
                    continue

                if (
                    allowed_filters
                    and entry_name not in allowed_filters
                    and entry_name not in critical_dimension_defaults
                ):
                    continue

                topn_filter_expressions.extend(build_filter_expression(entry, spec))

            requested_limit = max(1, min(int(state.get("limit", 5) or 5), 100))
            dax_limit = requested_limit
            if (group_by_entry.get("type") or "").lower() == "text":
                dax_limit = min(100, requested_limit + 25)

            dax_query = build_topn_dax(
                measure_expr,
                group_by_entry.get("column", ""),
                topn_filter_expressions,
                dax_limit,
                order=state.get("order", "DESC"),
            )
        elif query_type in {"barchart", "pie"}:
            group_by_entry = None
            if state.get("group_by"):
                group_by_entry = resolve_filter_name(
                    state.get("group_by", ""), resources.filter_entries
                )
            if not group_by_entry:
                return {
                    "response": (
                        "I could not determine the group-by field for the chart. "
                        "Please rephrase and include the field name."
                    )
                }
            if (
                allowed_filters
                and group_by_entry.get("name") not in allowed_filters
                and not using_template_group_by
            ):
                return {
                    "response": (
                        "The requested group-by field is not valid for this KPI. "
                        "Please choose a different field."
                    )
                }
            if (group_by_entry.get("type") or "").lower() == "date":
                dax_query = build_multi_measure_dax(
                    [(kpi_entry.get("name", "KPI"), measure_expr)],
                    group_by_entry.get("column", ""),
                    filter_expressions,
                    limit=state.get("limit", 100),
                    order="ASC",
                )
            else:
                dax_query = build_grouped_dax(
                    measure_expr,
                    group_by_entry.get("column", ""),
                    filter_expressions,
                    limit=state.get("limit", 10),
                    order=state.get("order", "DESC"),
                )
        elif query_type == "detail":
            template = detect_detail_template(state.get("question", ""), resources.visual_map)
            table_name = (template or {}).get("table") or state.get("table")
            column_names = (template or {}).get("columns", []) or state.get("columns")
            limit = (template or {}).get("limit", 50) or state.get("limit")
            if not table_name or not column_names:
                return {
                    "response": (
                        "I could not determine the detail table or columns. "
                        "Please rephrase and include the table and columns explicitly."
                    )
                }

            columns = []
            for col_name in column_names:
                entry = resolve_filter_name(col_name, resources.filter_entries)
                if entry:
                    columns.append(entry.get("column"))
                elif "[" in col_name and "]" in col_name:
                    columns.append(col_name)

            if not columns:
                return {
                    "response": (
                        "None of the requested columns are valid for the selected table. "
                        "Please choose different columns."
                    )
                }

            dax_query = build_detail_dax(
                table_name,
                columns,
                filter_expressions,
                limit=limit or 50,
            )
        else:
            dax_query = build_filtered_dax(measure_expr, filter_expressions)

        if not dax_query:
            return {"response": "Could not build the query for this request."}

        try:
            result = resources.pbi_tool.execute_query(dax_query)
            rls_meta = {}
            if isinstance(result, dict) and result.get("_rls_fallback"):
                rls_meta = {
                    "rls_fallback": True,
                    "rls_username": result.get("_rls_username"),
                    "rls_roles": result.get("_rls_roles"),
                }
                # Normalize shape to match Power BI executeQueries response
                result = {"results": result.get("results", [])}

            return {
                "query_result": result,
                "kpi_name": kpi_entry.get("name", ""),
                "query_type": query_type,
                "rejected_filters": rejected,
                **rls_meta,
            }
        except Exception as exc:
            return {"error": f"Error executing Power BI query: {exc}"}

    def format_response(state: AgentState) -> dict:
        if state.get("error"):
            return {"response": state["error"]}

        query_type = state.get("query_type", "kpi")
        if query_type == "comparison_chart":
            result = state.get("query_result", {})
            table = result.get("results", [{}])[0].get("tables", [{}])[0]
            rows = table.get("rows", [])
            columns = table.get("columns", [])

            x_axis = state.get("group_by", "Date")
            series = []
            x_values = []

            if rows:
                column_names = []
                if columns:
                    column_names = [col.get("name") for col in columns if col.get("name")]
                elif isinstance(rows[0], dict):
                    column_names = list(rows[0].keys())

                if not column_names:
                    payload = {
                        "type": state.get("chart_type", "line"),
                        "title": " vs ".join(state.get("comparison_measures", [])),
                        "x_axis": x_axis,
                        "x_values": [],
                        "series": [],
                    }
                    return {"response": json.dumps(payload, ensure_ascii=True)}

                x_col_name = column_names[0]
                x_values = [row.get(x_col_name) for row in rows if isinstance(row, dict)]

                for series_name in column_names[1:]:
                    series_data = [row.get(series_name) for row in rows if isinstance(row, dict)]
                    series.append({"name": series_name, "data": series_data})

                if x_values and all(isinstance(x, str) for x in x_values):
                    parsed = []
                    for idx, label in enumerate(x_values):
                        parsed_dt = None
                        try:
                            parsed_dt = datetime.fromisoformat(label.replace("Z", "+00:00"))
                        except ValueError:
                            tokens = [token.strip() for token in label.split("/")]
                            if (
                                len(tokens) == 2
                                and tokens[0].isdigit()
                                and tokens[1].isdigit()
                                and len(tokens[1]) == 4
                            ):
                                month = int(tokens[0])
                                year = int(tokens[1])
                                if 1 <= month <= 12:
                                    parsed_dt = datetime(year, month, 1)
                        if parsed_dt is None:
                            parsed = []
                            break
                        parsed.append((parsed_dt, idx))

                    if parsed and len(parsed) == len(x_values):
                        parsed.sort(key=lambda item: item[0])
                        order_idx = [item[1] for item in parsed]
                        x_values = [x_values[i] for i in order_idx]
                        for s in series:
                            data = s.get("data", [])
                            if len(data) == len(order_idx):
                                s["data"] = [data[i] for i in order_idx]

            payload = {
                "type": state.get("chart_type", "line"),
                "title": " vs ".join(state.get("comparison_measures", [])),
                "x_axis": x_axis,
                "x_values": x_values,
                "series": series,
            }
            response = json.dumps(payload, ensure_ascii=True)
            return {"response": response}

        if query_type == "comparison":
            return {}

        kpi_value = extract_kpi_value(state.get("query_result", {}))
        kpi_name = state.get("kpi_name") or "KPI"
        if not kpi_value and query_type == "kpi":
            return {"response": "No KPI value returned from Power BI."}

        table = (
            state.get("query_result", {})
            .get("results", [{}])[0]
            .get("tables", [{}])[0]
        )
        rows = table.get("rows", [])
        columns = table.get("columns", [])

        if query_type in {"topn", "barchart", "pie"}:
            pairs = []
            for row in rows:
                if isinstance(row, dict):
                    keys = list(row.keys())
                    if len(keys) >= 2:
                        pairs.append([row[keys[0]], float_safe(row[keys[1]])])
                elif isinstance(row, list) and len(row) >= 2:
                    pairs.append([row[0], float_safe(row[1])])
            kpi_entry = match_kpi_from_question(kpi_name, resources.glossary_entries) or {}
            measure_name = measure_name_from_expression(kpi_entry.get("measure", ""))
            normalized_title = normalize_text(kpi_name)
            is_percent_metric = (
                "%" in measure_name
                or "%" in kpi_name
                or "percent" in normalized_title
                or "percentage" in normalized_title
                or (
                    "otp" in normalized_title
                    and "min" not in normalized_title
                    and "minute" not in normalized_title
                )
            )
            if is_percent_metric:
                for pair in pairs:
                    try:
                        pair[1] = float(pair[1]) * 100.0
                    except Exception:
                        continue

            if query_type == "topn":
                pairs = [
                    pair
                    for pair in pairs
                    if pair[0] is not None
                    and normalize_text(str(pair[0])) not in {"", "blank", "(blank)", "null"}
                ]
                order = (state.get("order") or "DESC").upper()
                def _label_sort_key(value):
                    if value is None:
                        return (2, "")
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        return (0, int(value))
                    text = str(value).strip()
                    if text.isdigit():
                        try:
                            return (0, int(text))
                        except Exception:
                            pass
                    return (1, text.lower())

                def _topn_sort_key(pair):
                    metric = float_safe(pair[1])
                    label_key = _label_sort_key(pair[0])
                    if order == "ASC":
                        return (metric, label_key)
                    return (-metric, label_key)

                pairs.sort(key=_topn_sort_key)
                pairs = pairs[: state.get("limit", 10)]
            elif query_type == "barchart":
                parsed = []
                for pair in pairs:
                    label = pair[0]
                    if not isinstance(label, str):
                        parsed = []
                        break
                    parsed_dt = None
                    try:
                        parsed_dt = datetime.fromisoformat(label.replace("Z", "+00:00"))
                    except ValueError:
                        tokens = [token.strip() for token in label.split("/")]
                        if (
                            len(tokens) == 2
                            and tokens[0].isdigit()
                            and tokens[1].isdigit()
                            and len(tokens[1]) == 4
                        ):
                            month = int(tokens[0])
                            year = int(tokens[1])
                            if 1 <= month <= 12:
                                parsed_dt = datetime(year, month, 1)
                    if parsed_dt is None:
                        parsed = []
                        break
                    parsed.append((parsed_dt, pair))
                if parsed and len(parsed) == len(pairs):
                    parsed.sort(key=lambda item: item[0])
                    pairs = [item[1] for item in parsed]

            payload = {
                "type": "bar"
                if query_type == "barchart"
                else ("pie" if query_type == "pie" else "topn"),
                "title": kpi_name,
                "rows": pairs,
            }
            rejected = state.get("rejected_filters", [])
            if rejected:
                payload["ignored_filters"] = sorted(set(rejected))
            response = json.dumps(payload, ensure_ascii=True)
        elif query_type == "detail":
            normalized_rows = []
            if rows:
                if isinstance(rows[0], dict):
                    normalized_rows = rows
                elif isinstance(rows[0], list):
                    headers = []
                    if columns:
                        headers = [
                            col.get("name", f"Column{i+1}")
                            for i, col in enumerate(columns)
                        ]
                    else:
                        headers = [f"Column{i+1}" for i in range(len(rows[0]))]
                    for row in rows:
                        normalized_rows.append(
                            {headers[i]: row[i] for i in range(len(headers))}
                        )
            payload = {
                "type": "table",
                "title": kpi_name,
                "rows": normalized_rows,
            }
            rejected = state.get("rejected_filters", [])
            if rejected:
                payload["ignored_filters"] = sorted(set(rejected))
            response = json.dumps(payload, ensure_ascii=True)
        else:
            payload = {
                "type": "kpi",
                "title": kpi_name,
                "value": format_number(kpi_value),
            }
            kpi_entry = match_kpi_from_question(kpi_name, resources.glossary_entries) or {}
            measure_name = measure_name_from_expression(kpi_entry.get("measure", ""))
            normalized_title = normalize_text(kpi_name)
            is_percent_metric = (
                "%" in measure_name
                or "%" in kpi_name
                or "percent" in normalized_title
                or "percentage" in normalized_title
                or (
                    "otp" in normalized_title
                    and "min" not in normalized_title
                    and "minute" not in normalized_title
                )
            )
            if is_percent_metric:
                try:
                    numeric = float(kpi_value)
                    if -1.0 <= numeric <= 1.0:
                        payload["value"] = f"{numeric * 100.0:.2f}%"
                    else:
                        payload["value"] = f"{numeric:.2f}%"
                except Exception:
                    pass
            rejected = state.get("rejected_filters", [])
            if rejected:
                payload["ignored_filters"] = sorted(set(rejected))
            response = json.dumps(payload, ensure_ascii=True)

        # Attach RLS fallback info if present
        if state.get("rls_fallback"):
            note = {
                "rls_fallback_used": True,
                "rls_username": state.get("rls_username"),
                "rls_roles": state.get("rls_roles"),
                "note": "RLS identity was rejected (401); retried without effectiveIdentities.",
            }
            try:
                parsed = json.loads(response)
                parsed["_rls"] = note
                response = json.dumps(parsed, ensure_ascii=True)
            except Exception:
                # If response isn't JSON, append textual note
                response = f"{response}\n\n[RLS fallback used; identity rejected]"

        return {"response": response}

    graph = StateGraph(AgentState)
    graph.add_node("build_query", build_query)
    graph.add_node("run_query", run_query)
    graph.add_node("format_response", format_response)
    graph.set_entry_point("build_query")
    graph.add_edge("build_query", "run_query")
    graph.add_edge("run_query", "format_response")
    graph.add_edge("format_response", END)
    compiled = graph.compile()

    class VoiceEnabledAgent:
        """Wrapper that adds audio entrypoints while keeping existing text invoke intact."""

        def __init__(self, graph_runner, llm: AiDELLM):
            self._graph = graph_runner
            self._llm = llm

        def invoke(self, inputs: dict):
            """
            Accepts either:
            - {"question": "<text>"} (unchanged)
            - {"audio_bytes": b"...", "mime_type": "audio/wav", "prompt": "..."} to transcribe then run
            - {"audio_base64": "<base64>", "mime_type": "..."} alternative payload for web clients
            """
            if isinstance(inputs, dict):
                if "question" not in inputs:
                    audio_bytes = inputs.get("audio_bytes")
                    if not audio_bytes:
                        b64 = inputs.get("audio_base64")
                        if b64:
                            try:
                                audio_bytes = base64.b64decode(b64)
                            except Exception:
                                pass
                    if audio_bytes:
                        # Choose transport based on configuration (default http).
                        if self._llm.audio_transport == "realtime":
                            transcription = self._llm.invoke_audio_realtime(
                                audio_bytes,
                                input_format=inputs.get("input_audio_format"),
                                output_format=inputs.get("output_audio_format"),
                                prompt=inputs.get("prompt"),
                            )
                        else:
                            transcription = self._llm.invoke_audio(
                                audio_bytes,
                                mime_type=inputs.get("mime_type", "audio/pcm"),
                                input_audio_format=inputs.get("input_audio_format", "pcm16"),
                                prompt=inputs.get("prompt"),
                            )
                        if not (transcription or "").strip():
                            return {
                                "error": "No transcription returned from AiDE; check audio payload and input format.",
                                "response": "No transcription returned from AiDE; check audio payload and input format.",
                            }
                        # Merge but ensure question is present for downstream graph.
                        new_inputs = {**inputs, "question": transcription}
                        new_inputs.pop("audio_bytes", None)
                        new_inputs.pop("audio_base64", None)
                        new_inputs.pop("mime_type", None)
                        new_inputs.pop("prompt", None)
                        return self._graph.invoke(new_inputs)
            return self._graph.invoke(inputs)

        def invoke_audio(
            self,
            audio_bytes: bytes,
            mime_type: str = "audio/wav",
            prompt: str | None = None,
        ):
            """
            Convenience: transcribe audio, then run full KPI workflow.
            """
            transcription = self._llm.invoke_audio(audio_bytes, mime_type=mime_type, prompt=prompt)
            return self._graph.invoke({"question": transcription})

        def __getattr__(self, item):
            # Delegate any other runnable interfaces (e.g., astream) to the underlying graph.
            return getattr(self._graph, item)

    return VoiceEnabledAgent(compiled, resources.llm)

def handle_morning_ops_check(agent):
    """Handles the Morning Operations Check flow."""
    results = {"title": "Morning Operations Snapshot", "kpis": []}

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
            results["kpis"].append({"kpi": kpi, "value": f"Error: {result['error']}", "is_error": True})
        else:
            try:
                # The response from the agent is already a dict, not a JSON string
                response_data = result.get("response", {})
                if isinstance(response_data, str):
                    try:
                        # If for some reason it's a string, try to parse it
                        response_data = json.loads(response_data)
                    except json.JSONDecodeError:
                        response_data = {"value": response_data}
                
                if response_data.get("type") == "kpi":
                    title = response_data.get("title", kpi)
                    value = response_data.get("value", "N/A")
                    results["kpis"].append({"kpi": title, "value": value})
                else:
                    results["kpis"].append({"kpi": kpi, "value": response_data})
            except Exception as e:
                results["kpis"].append({"kpi": kpi, "value": f"Error processing response: {e}", "is_error": True})

    return results

def handle_otp_investigation(agent):
    """Handles the initial part of the OTP Investigation flow for the API."""
    results = {"title": "OTP Investigation", "kpis": []}
    
    otp_kpis = ["Pickup On-Time Performance", "Dropoff On-Time Performance"]

    for otp_kpi in otp_kpis:
        question = otp_kpi
        result = agent.invoke({"question": question})

        if result.get("error"):
            results["kpis"].append({"kpi": otp_kpi, "value": f"Error: {result['error']}", "is_error": True})
            continue

        try:
            response_data = result.get("response", {})
            if isinstance(response_data, str):
                try:
                    response_data = json.loads(response_data)
                except json.JSONDecodeError:
                    response_data = {"value": response_data}

            if response_data.get("type") == "kpi":
                title = response_data.get("title", otp_kpi)
                value = response_data.get("value", "N/A")
                results["kpis"].append({"kpi": title, "value": value})
            else:
                results["kpis"].append({"kpi": otp_kpi, "value": response_data})
        except Exception as e:
            results["kpis"].append({"kpi": otp_kpi, "value": f"Error processing response: {e}", "is_error": True})
            
    return results
