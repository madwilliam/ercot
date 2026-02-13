"""Example usage of PubApiClient with the ERCOT public API spec."""

from __future__ import annotations

import os
import sys

from client import ErcotPublicDataClient
from pubapi_client import PubApiClient


def main() -> int:
    api_key = os.getenv("ERCOT_API_KEY")
    if not api_key:
        print("Set ERCOT_API_KEY to your subscription key.", file=sys.stderr)
        return 1

    client = PubApiClient(api_key=api_key)

    # Optional ROPC auth step (if your environment requires it).
    username = os.getenv("ERCOT_USERNAME")
    password = os.getenv("ERCOT_PASSWORD")
    token = None
    if username and password:
        auth_client = ErcotPublicDataClient()
        token = auth_client.authenticate(username, password)
        print("Retrieved id_token.")

    # List a few available operationIds.
    print("First 10 operationIds:")
    for op_id in list(client.list_operations())[:10]:
        print(" -", op_id)

    # Call a simple endpoint that requires only the API key.
    if token:
        response = client.request(
            "GET",
            "/version",
            headers={"Authorization": f"Bearer {token}"},
        )
        version = response.json()
    else:
        version = client.call_json("getVersion")
    print("Version response:", version)

    # Example with path parameter (replace with a real emilId value).
    # emil = client.call_json("getEmilId", emilId="YOUR_EMIL_ID")
    # print("EMIL:", emil)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
