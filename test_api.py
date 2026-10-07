#!/usr/bin/env python3

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from urllib import error, parse, request
from uuid import uuid4


DEFAULT_API_VERSION = "2026-07-23"
BASE_URL = "https://openapi.dnse.com.vn"
ENDPOINT_PATH = "/price/ohlc"


def get_date_header_name():
    """Lấy tên header ngày; mặc định theo form DNSE đang dùng."""
    return os.getenv("DATE_HEADER", "X-Aux-Date")


def get_api_version():
    return os.getenv("DNSE_API_VERSION", DEFAULT_API_VERSION)


def build_signature(
    secret,
    method,
    path,
    date_value,
    algorithm="hmac-sha256",
    nonce=None,
    header_name=None,
):
    """Tạo giá trị headers và signature cho X-Signature."""
    header_name = header_name or get_date_header_name()
    header_key = header_name.lower()

    signed_headers = f"(request-target) {header_key}"

    signing_string = (
        f"(request-target): {method.lower()} {path}\n"
        f"{header_key}: {date_value}"
    )

    if nonce:
        signing_string += f"\nnonce: {nonce}"

    algorithms = {
        "hmac-sha256": hashlib.sha256,
        "hmac-sha384": hashlib.sha384,
        "hmac-sha512": hashlib.sha512,
        "hmac-sha1": hashlib.sha1,
    }

    digestmod = algorithms.get(algorithm.lower())
    if digestmod is None:
        raise ValueError(f"Thuật toán không được hỗ trợ: {algorithm}")

    mac = hmac.new(
        secret.encode("utf-8"),
        signing_string.encode("utf-8"),
        digestmod,
    )

    signature_base64 = base64.b64encode(mac.digest()).decode("ascii")
    encoded_signature = parse.quote(signature_base64, safe="")

    return signed_headers, encoded_signature


def send_signed_request(
    url,
    method,
    headers,
    body,
    api_key,
    api_secret,
    algorithm="hmac-sha256",
    hmac_nonce_enabled=True,
):
    """Ký và gửi request đến DNSE."""
    headers = dict(headers or {})
    headers.setdefault("version", get_api_version())
    headers.setdefault("X-Api-Key", api_key)

    parsed_url = parse.urlparse(url)

    # Theo mẫu SDK DNSE: ký path, không đưa query string vào request-target.
    path = parsed_url.path

    date_header_name = get_date_header_name()
    date_value = datetime.now(timezone.utc).strftime(
        "%a, %d %b %Y %H:%M:%S %z"
    )

    nonce = uuid4().hex if hmac_nonce_enabled else None

    signed_headers, encoded_signature = build_signature(
        secret=api_secret,
        method=method,
        path=path,
        date_value=date_value,
        algorithm=algorithm,
        nonce=nonce,
        header_name=date_header_name,
    )

    signature_header = (
        f'Signature keyId="{api_key}",'
        f'algorithm="{algorithm}",'
        f'headers="{signed_headers}",'
        f'signature="{encoded_signature}"'
    )

    if nonce:
        signature_header += f',nonce="{nonce}"'

    request_body = None
    if body is not None:
        request_body = json.dumps(body).encode("utf-8")
        headers.setdefault("Content-Type", "application/json")

    req = request.Request(
        url=url,
        data=request_body,
        method=method.upper(),
    )

    req.add_header(date_header_name, date_value)
    req.add_header("X-Signature", signature_header)

    for name, value in headers.items():
        req.add_header(name, str(value))

    # Không in API Key, Signature hoặc Secret ra console.
    print("Request URL:", url)
    print("Method:", method.upper())
    print("Date header:", date_header_name, date_value)
    print("Signed path:", path)
    print("Nonce enabled:", bool(nonce))

    try:
        with request.urlopen(req, timeout=30) as response:
            response_text = response.read().decode("utf-8")
            print("HTTP status:", response.status)

            try:
                print(json.dumps(json.loads(response_text), indent=2, ensure_ascii=False))
            except json.JSONDecodeError:
                print(response_text)

    except error.HTTPError as exc:
        response_text = exc.read().decode("utf-8", errors="replace")
        print("HTTP status:", exc.code)
        print(response_text)

    except error.URLError as exc:
        print("Không kết nối được đến DNSE:", exc.reason)


def main():
    api_key = os.getenv("DNSE_API_KEY")
    api_secret = os.getenv("DNSE_API_SECRET")

    if not api_key or not api_secret:
        raise SystemExit(
            "Thiếu credentials. Hãy đặt biến môi trường "
            "DNSE_API_KEY và DNSE_API_SECRET trước khi chạy."
        )
    start = int(datetime(2026, 9, 1, tzinfo=timezone.utc).timestamp())
    end = int(datetime(2026, 9, 8, tzinfo=timezone.utc).timestamp())

    # Thay khoảng thời gian này theo dữ liệu bạn muốn truy vấn.
    query_params = {
        "symbol": "ACB",
        "type": "STOCK",
        "resolution": "1D",
        "from": start,
        "to": end,
    }

    url = (
        f"{BASE_URL}{ENDPOINT_PATH}?"
        f"{parse.urlencode(query_params)}"
    )

    send_signed_request(
        url=url,
        method="GET",
        headers={},
        body=None,
        api_key=api_key,
        api_secret=api_secret,
        algorithm="hmac-sha256",
        hmac_nonce_enabled=True,
    )


if __name__ == "__main__":
    main()