import time
import requests


class PowerBITool:
    def __init__(
        self,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        dataset_id: str,
        workspace_id: str | None = None,
        impersonated_user: str | None = None,
        effective_username: str | None = None,
        effective_roles: list[str] | None = None,
        rls_enabled: bool = False,
        rls_username: str | None = None,
        rls_roles: list[str] | None = None,
    ):
        if not all([tenant_id, client_id, client_secret, dataset_id]):
            raise ValueError("Power BI REST configuration is incomplete.")

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.dataset_id = dataset_id
        self.workspace_id = workspace_id
        self.impersonated_user = impersonated_user
        self.effective_username = effective_username
        self.effective_roles = effective_roles or []
        self.rls_enabled = rls_enabled
        self.rls_username = rls_username
        self.rls_roles = rls_roles or []

        self._token = None
        self._token_expiry = 0

    def _get_access_token(self) -> str:
        if self._token and time.time() < (self._token_expiry - 60):
            return self._token

        token_url = (
            f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        )
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "client_credentials",
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        }

        try:
            response = requests.post(token_url, data=data, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            raise requests.exceptions.HTTPError(
                f"Failed to get access token: {http_err}. "
                f"Verify your tenant ID, client ID, and client secret are correct. "
                f"Response: {response.text}"
            )
        except Exception as exc:
            raise ConnectionError(
                "Failed to connect to the authentication endpoint. "
                "Check your internet connection and the token URL."
            ) from exc

        payload = response.json()
        self._token = payload["access_token"]
        self._token_expiry = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    def execute_query(self, dax_query: str) -> dict:
        """
        Executes a DAX query against the Power BI REST executeQueries endpoint.
        """
        token = self._get_access_token()
        if self.workspace_id:
            url = (
                "https://api.powerbi.com/v1.0/myorg/"
                f"groups/{self.workspace_id}/datasets/{self.dataset_id}/executeQueries"
            )
        else:
            url = (
                "https://api.powerbi.com/v1.0/myorg/"
                f"datasets/{self.dataset_id}/executeQueries"
            )

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        def build_payload(include_rls: bool = True) -> dict:
            payload = {
                "queries": [{"query": dax_query}],
                "serializerSettings": {"includeNulls": True},
            }
            if self.impersonated_user:
                payload["impersonatedUserName"] = self.impersonated_user

            if include_rls:
                identity_username = self.effective_username or self.rls_username
                identity_roles = self.effective_roles or self.rls_roles
                if identity_username and (self.rls_enabled or self.effective_username):
                    identity = {
                        "username": identity_username,
                        "datasets": [self.dataset_id],
                    }
                    if identity_roles:
                        identity["roles"] = list(identity_roles)
                    payload["effectiveIdentities"] = [identity]
                    payload["useUserPermission"] = False
            return payload

        payload = build_payload(include_rls=True)

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                response = requests.post(url, json=payload, headers=headers, timeout=60)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as http_err:
                status_code = getattr(response, "status_code", None)
                # If RLS is enabled and we get 401, retry once without effectiveIdentities
                if (
                    status_code == 401
                    and self.rls_enabled
                    and payload.get("effectiveIdentities")
                ):
                    try:
                        fallback_payload = build_payload(include_rls=False)
                        response = requests.post(
                            url, json=fallback_payload, headers=headers, timeout=60
                        )
                        response.raise_for_status()
                        # Indicate RLS fallback was used
                        return {
                            "_rls_fallback": True,
                            "_rls_username": payload["effectiveIdentities"][0].get("username"),
                            "_rls_roles": payload["effectiveIdentities"][0].get("roles"),
                            "results": response.json().get("results", []),
                        }
                    except Exception:
                        # If fallback also fails, continue to normal error handling
                        pass
                if status_code and status_code >= 500 and attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise requests.exceptions.HTTPError(
                    f"Query execution failed: {http_err}. "
                    f"Ensure the Power BI Service Principal is configured correctly and has "
                    f"the required permissions on the dataset. Response: {response.text}"
                )
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise ConnectionError(
                    "Failed to connect to the Power BI endpoint. "
                    "Check your internet connection and the API URL."
                ) from exc
            except Exception as exc:
                last_exc = exc
                break
        raise ConnectionError(
            "Failed to connect to the Power BI endpoint. "
            "Check your internet connection and the API URL."
        ) from last_exc
