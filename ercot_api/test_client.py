from __future__ import annotations

import json

from client import ErcotPublicDataClient
import zipfile
import io
import os
import glob
from tqdm import tqdm
class FakeResponse:
    def __init__(self, payload=None, status_code=200, content=b""):
        self._payload = payload if payload is not None else {}
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self):
        self.requests = []
        self._next_response = FakeResponse()
        self._next_post_response = FakeResponse()

    def queue_response(self, payload=None, status_code=200, content=b""):
        self._next_response = FakeResponse(payload=payload, status_code=status_code, content=content)

    def queue_post_response(self, payload=None, status_code=200, content=b""):
        self._next_post_response = FakeResponse(payload=payload, status_code=status_code, content=content)

    def request(self, method, url, headers=None, params=None, json=None, timeout=None, stream=False):
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers or {}),
                "params": dict(params or {}),
                "json": json,
                "timeout": timeout,
                "stream": stream,
            }
        )
        return self._next_response

    def post(self, url, data=None, params=None, headers=None, timeout=None):
        self.requests.append(
            {
                "method": "POST",
                "url": url,
                "data": dict(data or {}),
                "params": dict(params or {}),
                "headers": dict(headers or {}),
                "timeout": timeout,
            }
        )
        return self._next_post_response


def test_header_api_key():
    session = FakeSession()
    session.queue_response(payload={"ok": True})
    client = ErcotPublicDataClient(api_key="abc123", session=session)
    data = client.get_version()
    assert data["ok"] is True
    req = session.requests[-1]
    assert req["headers"].get("Ocp-Apim-Subscription-Key") == "abc123"
    assert "subscription-key" not in req["params"]


def test_query_api_key():
    session = FakeSession()
    session.queue_response(payload={"ok": True})
    client = ErcotPublicDataClient(api_key="abc123", api_key_in_query=True, session=session)
    client.list_products()
    req = session.requests[-1]
    assert "Ocp-Apim-Subscription-Key" not in req["headers"]
    assert req["params"].get("subscription-key") == "abc123"


def test_authenticate_sets_token():
    session = FakeSession()
    session.queue_post_response(payload={"id_token": "tok123"})
    client = ErcotPublicDataClient(session=session)
    token = client.authenticate("user", "pass")
    assert token == "tok123"
    assert client._headers().get("Authorization") == "Bearer tok123"
    req = session.requests[-1]
    assert "username=user" in req["url"]
    assert "password=pass" in req["url"]
    assert "response_type=id_token" in req["url"]


def test_zip_download():
    session = FakeSession()
    session.queue_response(content=b"zipbytes")
    client = ErcotPublicDataClient(session=session)
    data = client.download_archives("product", [1, 2, 3])
    assert data == b"zipbytes"
    req = session.requests[-1]
    assert req["method"] == "POST"
    assert req["json"] == {"docIds": [1, 2, 3]}
    assert req["stream"] is True


def test_iter_report_pages_stops():
    session = FakeSession()
    client = ErcotPublicDataClient(session=session)

    pages = [
        {"_meta": {"totalPages": 2, "currentPage": 0}, "data": [1]},
        {"_meta": {"totalPages": 2, "currentPage": 1}, "data": [2]},
    ]
    out = []
    for payload in pages:
        session.queue_response(payload=payload)
        out.extend(list(client.iter_report_pages("/rpt", page_size=1, max_pages=1)))
        break
        assert out[0]["data"] == [1]


if __name__ == "__main__":
    USERNAME = "williamzhongkaiwu@gmail.com"
    PASSWORD = "Wyslmwinlyab5225"
    # client = ErcotPublicDataClient(api_key="a2255a08961b41f187af0e7a248fb2d7", api_key_in_query=False)
    client = ErcotPublicDataClient(api_key="2149cf06d17d456bbe98e98fffa60ac0", api_key_in_query=False)
    client.authenticate(USERNAME, PASSWORD)
    products = client.list_products()
    product_id = 'NP6-905-CD'
    report = client.get_product_history_bundles(product_id)
    ids = [i['docId'] for i in report['bundles']]
    max_retry = 3
    for report_id in tqdm(ids):
        done = False
        retry = 0
        while not done:
            try:
                zip = client.download_bundle(product_id, [report_id])
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
