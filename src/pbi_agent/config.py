from dataclasses import dataclass
import os

from dotenv import load_dotenv


@dataclass
class AppConfig:
    aide_api_token: str
    aide_base_url: str
    aide_configuration_profile: str | None
    aide_model: str
    aide_audio_model: str
    aide_audio_path: str
    aide_realtime_path: str
    aide_input_audio_format: str
    aide_output_audio_format: str
    aide_server_vad: bool
    aide_ssl_verify: bool | str
    aide_audio_transport: str
    tenant_id: str
    client_id: str
    client_secret: str
    dataset_id: str
    workspace_id: str | None
    impersonated_user: str | None
    effective_username: str | None
    effective_roles: list[str]
    rls_enabled: bool
    rls_username: str | None
    rls_roles: list[str]
    server_name_filter: str | None
    glossary_path: str
    filter_glossary_path: str
    kpi_filter_map_path: str
    report_defaults_path: str
    report_page_defaults_path: str
    visual_map_path: str


def load_config(project_root: str) -> AppConfig:
    load_dotenv()

    aide_api_token = os.getenv("AIDE_API_TOKEN")
    if not aide_api_token:
        raise ValueError(
            "AiDE API token is not set. Please configure AIDE_API_TOKEN in your .env file."
        )
    aide_base_url = os.getenv(
        "AIDE_BASE_URL", "https://aide.transit-technologies.ai/api/v1/brownfield/"
    )
    aide_configuration_profile = os.getenv("AIDE_CONFIGURATION_PROFILE", "Transit")
    aide_model = os.getenv("AIDE_MODEL", "gpt-4o-mini")
    aide_audio_model = os.getenv("AIDE_AUDIO_MODEL", "gpt-realtime-mini")
    aide_audio_path = os.getenv("AIDE_AUDIO_PATH", "completions")
    aide_realtime_path = os.getenv("AIDE_REALTIME_PATH", "realtime")
    aide_input_audio_format = os.getenv("AIDE_INPUT_AUDIO_FORMAT", "pcm16")
    aide_output_audio_format = os.getenv("AIDE_OUTPUT_AUDIO_FORMAT", "opus")
    aide_server_vad = os.getenv("AIDE_SERVER_VAD", "false").lower() in {"1", "true", "yes", "y"}
    ssl_env = os.getenv("AIDE_SSL_VERIFY")
    if ssl_env is None:
        aide_ssl_verify: bool | str = True
    elif ssl_env.lower() in {"0", "false", "no", "off"}:
        aide_ssl_verify = False
    else:
        # Treat as path to CA bundle
        aide_ssl_verify = ssl_env
    aide_audio_transport = os.getenv("AIDE_AUDIO_TRANSPORT", "http")

    tenant_id = os.getenv("POWERBI_TENANT_ID")
    client_id = os.getenv("POWERBI_CLIENT_ID")
    client_secret = os.getenv("POWERBI_CLIENT_SECRET")
    dataset_id = os.getenv("POWERBI_DATASET_ID")
    workspace_id = os.getenv("POWERBI_WORKSPACE_ID")
    impersonated_user = os.getenv("POWERBI_IMPERSONATED_USER")
    effective_username = os.getenv("POWERBI_EFFECTIVE_USERNAME")
    roles_raw = os.getenv("POWERBI_EFFECTIVE_ROLES") or ""
    effective_roles = [r.strip() for r in roles_raw.split(",") if r.strip()]
    server_name_filter = os.getenv("POWERBI_SERVER_NAME_FILTER")
    rls_enabled = os.getenv("POWERBI_RLS_ENABLED", "false").lower() in {"1", "true", "yes", "y"}
    rls_username = os.getenv("POWERBI_RLS_USERNAME")
    rls_roles_raw = os.getenv("POWERBI_RLS_ROLES") or ""
    rls_roles = [r.strip() for r in rls_roles_raw.split(",") if r.strip()]

    if not all([tenant_id, client_id, client_secret, dataset_id]):
        raise ValueError(
            "Power BI environment variables are not set. Please configure "
            "POWERBI_TENANT_ID, POWERBI_CLIENT_ID, POWERBI_CLIENT_SECRET, "
            "and POWERBI_DATASET_ID in your .env file."
        )

    glossary_path = os.getenv(
        "KPI_GLOSSARY_PATH",
        os.path.join(project_root, "src", "pbi_agent", "kpi_glossary.json"),
    )
    filter_glossary_path = os.getenv(
        "FILTER_GLOSSARY_PATH",
        os.path.join(project_root, "src", "pbi_agent", "filter_glossary.json"),
    )
    kpi_filter_map_path = os.getenv(
        "KPI_FILTER_MAP_PATH",
        os.path.join(project_root, "src", "pbi_agent", "kpi_filter_map.json"),
    )
    report_defaults_path = os.getenv(
        "REPORT_DEFAULTS_PATH",
        os.path.join(project_root, "src", "pbi_agent", "report_defaults.json"),
    )
    report_page_defaults_path = os.getenv(
        "REPORT_PAGE_DEFAULTS_PATH",
        os.path.join(project_root, "src", "pbi_agent", "report_page_defaults.json"),
    )
    visual_map_path = os.getenv(
        "VISUAL_MAP_PATH",
        os.path.join(project_root, "src", "pbi_agent", "visual_map.json"),
    )

    return AppConfig(
        aide_api_token=aide_api_token,
        aide_base_url=aide_base_url,
        aide_configuration_profile=aide_configuration_profile,
        aide_model=aide_model,
        aide_audio_model=aide_audio_model,
        aide_audio_path=aide_audio_path,
        aide_realtime_path=aide_realtime_path,
        aide_input_audio_format=aide_input_audio_format,
        aide_output_audio_format=aide_output_audio_format,
        aide_server_vad=aide_server_vad,
        aide_ssl_verify=aide_ssl_verify,
        aide_audio_transport=aide_audio_transport,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        impersonated_user=impersonated_user,
        effective_username=effective_username,
        effective_roles=effective_roles,
        rls_enabled=rls_enabled,
        rls_username=rls_username,
        rls_roles=rls_roles,
        server_name_filter=server_name_filter,
        glossary_path=glossary_path,
        filter_glossary_path=filter_glossary_path,
        kpi_filter_map_path=kpi_filter_map_path,
        report_defaults_path=report_defaults_path,
        report_page_defaults_path=report_page_defaults_path,
        visual_map_path=visual_map_path,
    )
