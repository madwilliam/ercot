"""Generic wrapper for the ERCOT Public API spec (pubapi-apim-api.json)."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import requests


def _default_spec_path() -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "api-specs" / "pubapi" / "pubapi-apim-api.json"


@dataclass
class PubApiClientConfig:
    base_url: str
    timeout: float = 30.0
    api_key: Optional[str] = None
    api_key_in_query: bool = False
    spec_path: Optional[Path] = None


class PubApiClient:
    """Generic client driven by the OpenAPI spec's operationId values."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_key_in_query: bool = False,
        base_url: Optional[str] = None,
        timeout: float = 30.0,
        spec_path: Optional[str | Path] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        spec_file = Path(spec_path) if spec_path else _default_spec_path()
        spec = json.loads(spec_file.read_text())
        resolved_base = base_url or (spec.get("servers") or [{}])[0].get("url")
        if not resolved_base:
            raise ValueError("base_url must be provided or present in the spec servers list")

        self.config = PubApiClientConfig(
            base_url=resolved_base,
            timeout=timeout,
            api_key=api_key,
            api_key_in_query=api_key_in_query,
            spec_path=spec_file,
        )
        self._spec = spec
        self._operations = self._index_operations(spec)
        self._session = session or requests.Session()

    @staticmethod
    def _index_operations(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        operations: Dict[str, Dict[str, Any]] = {}
        for path, methods in spec.get("paths", {}).items():
            for method, op in methods.items():
                if method.startswith("x-"):
                    continue
                operation_id = op.get("operationId")
                if not operation_id:
                    continue
                operations[operation_id] = {
                    "method": method.upper(),
                    "path": path,
                    "parameters": op.get("parameters", []),
                    "requestBody": op.get("requestBody"),
                }
        return operations

    def list_operations(self) -> Iterable[str]:
        return sorted(self._operations.keys())

    def _headers(self, extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers: Dict[str, str] = dict(extra or {})
        if self.config.api_key and not self.config.api_key_in_query:
            headers["Ocp-Apim-Subscription-Key"] = self.config.api_key
        return headers

    def _query_api_key(self, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        params = dict(params or {})
        if self.config.api_key and self.config.api_key_in_query:
            params["subscription-key"] = self.config.api_key
        return params

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        stream: bool = False,
    ) -> requests.Response:
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        response = self._session.request(
            method=method,
            url=url,
            headers=self._headers(headers),
            params=self._query_api_key(params),
            json=json_body,
            data=data,
            timeout=self.config.timeout,
            stream=stream,
        )
        response.raise_for_status()
        return response

    def call(self, operation_id: str, **kwargs: Any) -> requests.Response:
        op = self._operations.get(operation_id)
        if not op:
            raise KeyError(f"Unknown operationId: {operation_id}")

        path_params = {p["name"] for p in op["parameters"] if p.get("in") == "path"}
        query_params = {p["name"] for p in op["parameters"] if p.get("in") == "query"}
        header_params = {p["name"] for p in op["parameters"] if p.get("in") == "header"}

        missing = [name for name in path_params if name not in kwargs]
        if missing:
            raise ValueError(f"Missing required path parameters: {', '.join(missing)}")

        path_values = {name: kwargs.pop(name) for name in path_params}
        query = {name: kwargs.pop(name) for name in query_params if name in kwargs}
        headers = {name: kwargs.pop(name) for name in header_params if name in kwargs}

        json_body = kwargs.pop("json", None)
        data = kwargs.pop("data", None)
        if op.get("requestBody") and json_body is None and data is None:
            json_body = kwargs.pop("body", None)

        if kwargs:
            unknown = ", ".join(sorted(kwargs.keys()))
            raise ValueError(f"Unexpected parameters for {operation_id}: {unknown}")

        path = op["path"].format(**path_values)
        return self.request(
            op["method"],
            path,
            params=query,
            headers=headers or None,
            json_body=json_body,
            data=data,
        )

    def call_json(self, operation_id: str, **kwargs: Any) -> Dict[str, Any]:
        return self.call(operation_id, **kwargs).json()
