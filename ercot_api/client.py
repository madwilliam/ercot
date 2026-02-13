"""ERCOT Public Data API client wrapper.

Based on OpenAPI 3.0.1 spec snippet provided by the user.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Deque, Dict, Iterable, Optional
from urllib.parse import quote_plus, urlencode
from collections import deque
import zipfile
import io
import os
import glob
import threading
import time
import requests
from tqdm import tqdm

BASE = "https://api.ercot.com/api/public-data"
BASE1 = "https://api.ercot.com/api/public-reports"

@dataclass
class ErcotClientConfig:
    base_url: str = BASE1
    timeout: float = 30.0
    api_key: Optional[str] = None
    api_key_in_query: bool = False
    rate_limit_per_minute: Optional[int] = 30
    rate_limit_window_seconds: float = 60.0
    token_url: str = (
        "https://ercotb2c.b2clogin.com/"
        "ercotb2c.onmicrosoft.com/"
        "B2C_1_PUBAPI-ROPC-FLOW/oauth2/v2.0/token"
    )
    client_id: str = "fec253ea-0d06-4272-a5e6-b478baeecd70"
    scope: str = "openid fec253ea-0d06-4272-a5e6-b478baeecd70 offline_access"


class ErcotPublicDataClient:
    """Thin wrapper around the ERCOT Public Data API.

    This client supports both header and query-string API keys:
    - Header: Ocp-Apim-Subscription-Key
    - Query: subscription-key
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
        api_key_in_query: bool = False,
        rate_limit_per_minute: Optional[int] = None,
        rate_limit_window_seconds: Optional[float] = None,
        token_url: Optional[str] = None,
        client_id: Optional[str] = None,
        scope: Optional[str] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.config = ErcotClientConfig(
            base_url=base_url or ErcotClientConfig.base_url,
            timeout=timeout,
            api_key=api_key,
            api_key_in_query=api_key_in_query,
            rate_limit_per_minute=(
                rate_limit_per_minute
                if rate_limit_per_minute is not None
                else ErcotClientConfig.rate_limit_per_minute
            ),
            rate_limit_window_seconds=(
                rate_limit_window_seconds
                if rate_limit_window_seconds is not None
                else ErcotClientConfig.rate_limit_window_seconds
            ),
            token_url=token_url or ErcotClientConfig.token_url,
            client_id=client_id or ErcotClientConfig.client_id,
            scope=scope or ErcotClientConfig.scope,
        )
        self._session = session or requests.Session()
        self._id_token: Optional[str] = None
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_timestamps: Deque[float] = deque()

    def _headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self.config.api_key and not self.config.api_key_in_query:
            headers["Ocp-Apim-Subscription-Key"] = self.config.api_key
        if self._id_token:
            headers["Authorization"] = f"Bearer {self._id_token}"
        return headers

    def _query_api_key(self, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        params = dict(params or {})
        if self.config.api_key and self.config.api_key_in_query:
            params["subscription-key"] = self.config.api_key
        return params

    def _wait_for_rate_limit(self) -> None:
        limit = self.config.rate_limit_per_minute
        if not limit or limit <= 0:
            return
        window = self.config.rate_limit_window_seconds
        while True:
            with self._rate_limit_lock:
                now = time.monotonic()
                cutoff = now - window
                while self._rate_limit_timestamps and self._rate_limit_timestamps[0] <= cutoff:
                    self._rate_limit_timestamps.popleft()
                if len(self._rate_limit_timestamps) < limit:
                    self._rate_limit_timestamps.append(now)
                    return
                sleep_for = (self._rate_limit_timestamps[0] + window) - now
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                time.sleep(0)

    def _request(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None,
                 json: Optional[Dict[str, Any]] = None, stream: bool = False) -> requests.Response:
        self._wait_for_rate_limit()
        url = f"{self.config.base_url.rstrip('/')}/{path.lstrip('/')}"
        response = self._session.request(
            method=method,
            url=url,
            headers=self._headers(),
            params=self._query_api_key(params),
            json=json,
            timeout=self.config.timeout,
            stream=stream,
        )
        response.raise_for_status()
        return response

    def authenticate(self, username: str, password: str) -> str:
        """Authenticate via ROPC flow and store the id_token for subsequent calls."""
        payload = {
            "username": username,
            "password": password,
            "grant_type": "password",
            "scope": self.config.scope,
            "client_id": self.config.client_id,
            "response_type": "id_token",
        }
        query = urlencode(payload, quote_via=quote_plus)
        url = f"{self.config.token_url}?{query}"
        response = self._session.post(
            url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=self.config.timeout,
        )
        response.raise_for_status()
        data = response.json()
        token = data.get("id_token")
        if not token:
            raise RuntimeError("Token response did not include id_token")
        self._id_token = token
        return token

    def set_id_token(self, token: str) -> None:
        """Manually set an id_token (if you already retrieved it)."""
        self._id_token = token

    def _get_json(self, path: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request("GET", path, params=params).json()

    def _post_json(self, path: str, *, params: Optional[Dict[str, Any]] = None,
                   json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self._request("POST", path, params=params, json=json).json()

    def _post_zip(self, path: str, *, json: Dict[str, Any]) -> bytes:
        response = self._request("POST", path, json=json, stream=True)
        return response.content

    # ---- Common endpoints ----
    def get_version(self) -> Dict[str, Any]:
        """GET /version"""
        return self._get_json("/version")

    def list_products(self) -> Dict[str, Any]:
        """GET /"""
        return self._get_json("/")

    def get_product(self, product_id: str) -> Dict[str, Any]:
        """GET /{productId}"""
        return self._get_json(f"/{product_id}")

    def get_product_history(self, product_id: str, *, page: Optional[int] = None,
                            size: Optional[int] = None) -> Dict[str, Any]:
        """GET /archive/{productId}"""
        params: Dict[str, Any] = {}
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        return self._get_json(f"/archive/{product_id}", params=params)

    def get_product_history_bundles(self, product_id: str) -> Dict[str, Any]:
        """GET /bundle/{productId}"""
        return self._get_json(f"/bundle/{product_id}")

    def download_archives(self, product_id: str, doc_ids: Iterable[int]) -> bytes:
        """POST /archive/{productId}/download (returns ZIP bytes)."""
        payload = {"docIds": list(doc_ids)}
        return self._post_zip(f"/archive/{product_id}/download", json=payload)

    def download_bundle(self, product_id: str, doc_ids: Iterable[int]) -> bytes:
        """POST /bundle/{productId}/download (returns ZIP bytes)."""
        payload = {"docIds": list(doc_ids)}
        return self._post_zip(f"/bundle/{product_id}/download", json=payload)

    # ---- Example product-specific endpoint from the spec ----
    def get_rptesr_m_four_sec_esr_charging_mw(
        self,
        *,
        agc_exec_time_from: Optional[str] = None,
        agc_exec_time_to: Optional[str] = None,
        dst_flag: Optional[bool] = None,
        agc_exec_time_utc_from: Optional[str] = None,
        agc_exec_time_utc_to: Optional[str] = None,
        system_demand_from: Optional[float] = None,
        system_demand_to: Optional[float] = None,
        esr_charging_mw_from: Optional[float] = None,
        esr_charging_mw_to: Optional[float] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        sort: Optional[str] = None,
        direction: Optional[str] = None,
    ) -> Dict[str, Any]:
        """GET /rptesr-m/4_sec_esr_charging_mw"""
        params: Dict[str, Any] = {}
        if agc_exec_time_from:
            params["AGCExecTimeFrom"] = agc_exec_time_from
        if agc_exec_time_to:
            params["AGCExecTimeTo"] = agc_exec_time_to
        if dst_flag is not None:
            params["DSTFlag"] = str(dst_flag).lower()
        if agc_exec_time_utc_from:
            params["AGCExecTimeUTCFrom"] = agc_exec_time_utc_from
        if agc_exec_time_utc_to:
            params["AGCExecTimeUTCTo"] = agc_exec_time_utc_to
        if system_demand_from is not None:
            params["systemDemandFrom"] = system_demand_from
        if system_demand_to is not None:
            params["systemDemandTo"] = system_demand_to
        if esr_charging_mw_from is not None:
            params["ESRChargingMWFrom"] = esr_charging_mw_from
        if esr_charging_mw_to is not None:
            params["ESRChargingMWTo"] = esr_charging_mw_to
        if page is not None:
            params["page"] = page
        if size is not None:
            params["size"] = size
        if sort:
            params["sort"] = sort
        if direction:
            params["dir"] = direction
        return self._get_json("/rptesr-m/4_sec_esr_charging_mw", params=params)

    # ---- Helper for repeated pagination patterns ----
    def iter_report_pages(self, path: str, *, params: Optional[Dict[str, Any]] = None,
                          page_size: int = 1000, max_pages: Optional[int] = None) -> Iterable[Dict[str, Any]]:
        """Iterate pages for report endpoints that accept page/size."""
        page = 0
        fetched = 0
        while True:
            merged_params = dict(params or {})
            merged_params["page"] = page
            merged_params["size"] = page_size
            data = self._get_json(path, params=merged_params)
            yield data
            fetched += 1
            if max_pages is not None and fetched >= max_pages:
                return
            meta = data.get("_meta", {})
            total_pages = meta.get("totalPages")
            current_page = meta.get("currentPage")
            if total_pages is None or current_page is None:
                return
            if current_page + 1 >= total_pages:
                return
            page += 1

    def update_archive(self,product_id = 'np4-33-cd'):
        report = self.get_product_history_bundles(product_id)
        ids = [i['docId'] for i in report['bundles']]
        max_retry = 3
        for report_id in tqdm(ids):
            done = False
            retry = 0
            while not done:
                try:
                    zip = self.download_bundle(product_id, [report_id])
                    bytes_io = io.BytesIO(zip)
                    save_dir = f'/home/dell/code/ercot/ercot/data/{product_id}/{product_id}_{str(report_id)}'
                    if not os.path.exists(save_dir):
                        os.mkdir(save_dir)
                        with zipfile.ZipFile(bytes_io, 'r') as zip_ref:
                            zip_ref.extractall(save_dir)
                        files = os.listdir(save_dir)
                        for filei in files:
                            file_path = os.path.join(save_dir,filei)
                            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                                zip_ref.extractall(save_dir)
                        path_pattern = f"{save_dir}/*.zip"
                        for file_path in glob.glob(path_pattern):
                            os.remove(file_path)
                            print(f"Deleted: {file_path}")
                    else:
                        print(f'{report_id} already exist')
                    done = True
                except:
                    if retry<max_retry:
                        print('retrying')
                    else:
                        break
                    retry+=1
