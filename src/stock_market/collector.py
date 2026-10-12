
import base64
import hashlib
import hmac
import json
import os
import re
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib import error, parse, request
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg2
import requests

import boto3
from botocore.config import Config

BASE_URL = "https://openapi.dnse.com.vn"
OHLC_PATH = "/price/ohlc"
INSTRUMENTS_PATH = "/market/instruments"
DEFAULT_API_VERSION = "2026-07-23"
LOCAL_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _signature(secret: str, method: str, path: str, date_value: str, nonce: str):

    date_header = os.getenv("DATE_HEADER", "X-Aux-Date")
    canonical_header = date_header.lower()
    signing_text = (
        f"(request-target): {method.lower()} {path}\n"
        f"{canonical_header}: {date_value}\n"
        f"nonce: {nonce}"
    )
    digest = hmac.new(
        secret.encode("utf-8"), signing_text.encode("utf-8"), hashlib.sha256
    ).digest()
    encoded = base64.b64encode(digest).decode("ascii")
    encoded = encoded.replace("+", "%2B").replace("/", "%2F").replace("=", "%3D")
    return date_header, canonical_header, encoded


def _request_json(path: str, params: dict | None = None) -> dict | list:
    api_key = _required_env("DNSE_API_KEY")
    api_secret = _required_env("DNSE_API_SECRET")
    nonce = uuid4().hex
    date_value = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S %z")
    date_header, canonical_header, encoded_signature = _signature(
        api_secret, "GET", path, date_value, nonce
    )
    url = f"{BASE_URL}{path}"
    if params:
        url += f"?{parse.urlencode(params)}"

    signature_header = (
        f'Signature keyId="{api_key}",algorithm="hmac-sha256",'
        f'headers="(request-target) {canonical_header}",'
        f'signature="{encoded_signature}",nonce="{nonce}"'
    )
    headers = {
        "X-Api-Key": api_key,
        "X-Signature": signature_header,
        date_header: date_value,
        "version": os.getenv("DNSE_API_VERSION", DEFAULT_API_VERSION),
        "Accept": "application/json",
    }
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=30) as response:
            body = response.read().decode("utf-8")
            if response.status < 200 or response.status >= 300:
                raise RuntimeError(f"DNSE returned HTTP {response.status}: {body}")
            try:
                return json.loads(body)
            except json.JSONDecodeError as exc:
                raise RuntimeError("DNSE response is not valid JSON") from exc
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DNSE returned HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Could not connect to DNSE: {exc.reason}") from exc


def _request_ohlc(
    symbol: str, resolution: str, start_ts: int, end_ts: int, market_type: str = "STOCK"
) -> dict | list:
    return _request_json(
        OHLC_PATH,
        {
            "symbol": symbol,
            "type": market_type,
            "resolution": resolution,
            "from": start_ts,
            "to": end_ts,
        },
    )


def _request_instruments(symbols: list[str]) -> dict | list:
    # DNSE's official SDK demonstrates these filters and pagination fields.
    # Limit the snapshot to the configured universe rather than ingesting an
    # arbitrary default page of the full instrument catalog.
    return _request_json(
        INSTRUMENTS_PATH,
        {
            "symbol": ",".join(symbols),
            "market_id": "",
            "security_group_id": "",
            "index_name": "",
            "limit": 100,
            "page": 1,
        },
    )


def _request_instrument_page(page: int, limit: int) -> dict | list:
    
    return _request_json(
        INSTRUMENTS_PATH,
        {
            "symbol": "",
            "market_id": "",
            "security_group_id": "",
            "index_name": "",
            "limit": limit,
            "page": page,
        },
    )


def _extract_instrument_rows(payload: dict | list) -> list[dict]:
    
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "instruments", "items", "results", "content"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _extract_instrument_rows(value)
            if nested:
                return nested
    return []


def _instrument_symbol(row: dict) -> str | None:
    for key in ("symbol", "ticker", "code", "instrumentCode", "instrument_code"):
        value = row.get(key)
        if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9]{2,12}", value.strip()):
            return value.strip().upper()
    return None


def _instrument_security_group(row: dict) -> str | None:
    for key in ("securityGroupId", "security_group_id", "securityGroup", "security_group"):
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip().upper()
    return None


def _request_instrument_universe(page_size: int, max_pages: int) -> tuple[list[dict], list[dict]]:
    """Fetch pages until the last short page, repeated page, or safety cap."""
    all_rows: list[dict] = []
    page_envelopes: list[dict] = []
    seen_symbols: set[str] = set()
    previous_fingerprints: set[tuple[str, ...]] = set()

    for page in range(1, max_pages + 1):
        payload = _request_instrument_page(page, page_size)
        rows = _extract_instrument_rows(payload)
        if not rows:
            # Preserve even an unfamiliar response shape in Raw so its schema
            # can be inspected without repeating the API call.
            if payload:
                page_envelopes.append({"page": page, "response": payload, "record_count": 0})
            break

        fingerprint = tuple(_instrument_symbol(row) or str(row) for row in rows)
        if fingerprint in previous_fingerprints:
            print(f"Instrument API repeated page {page}; stop pagination")
            break
        previous_fingerprints.add(fingerprint)
        page_envelopes.append({"page": page, "response": payload, "record_count": len(rows)})
        for row in rows:
            symbol = _instrument_symbol(row)
            if symbol and symbol not in seen_symbols:
                all_rows.append(row)
                seen_symbols.add(symbol)
        if len(rows) < page_size:
            break
    else:
        print(f"Instrument pagination reached configured safety cap: {max_pages} pages")

    return all_rows, page_envelopes


def _response_record_count(payload: dict | list) -> int:
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("t", "data", "instruments", "items", "results", "content"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
    return 0


def _date_window(conf: dict) -> tuple[date, date]:
   
    today = datetime.now(LOCAL_TZ).date()
    start_text = conf.get("from_date")
    end_text = conf.get("to_date")
    if start_text or end_text:
        if not start_text or not end_text:
            raise ValueError("Provide both dag_run.conf from_date and to_date (YYYY-MM-DD)")
        start_date = date.fromisoformat(start_text)
        end_date = date.fromisoformat(end_text)
    else:
        # Re-fetch a rolling window so weekends/holidays and late corrections are covered.
        start_date = today - timedelta(days=7)
        end_date = today
    if start_date > end_date:
        raise ValueError("from_date must be on or before to_date")
    return start_date, end_date


def _to_epoch_seconds(local_date: date) -> int:
    local_midnight = datetime.combine(local_date, time.min, tzinfo=LOCAL_TZ)
    return int(local_midnight.astimezone(timezone.utc).timestamp())


def _safe_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.=-]+", "_", value).strip("_") or "run"


def _raw_relative_path(
    symbol: str, resolution: str, run_id: str, ingest_date: date, entity: str = "ohlc"
) -> str:
    return (
        f"/source=dnse/entity={_safe_component(entity)}/ingest_date={ingest_date.isoformat()}"
        f"/symbol={_safe_component(symbol)}/resolution={_safe_component(resolution)}"
        f"/{_safe_component(run_id)}.json"
    )


def _write_raw_local(payload: dict, relative_path: str) -> str:
    root = Path(os.getenv("RAW_LOCAL_ROOT", "/opt/airflow/data/raw"))
    path = root.joinpath(*relative_path.strip("/").split("/"))
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    temp_path.replace(path)
    return str(path)


def _write_raw_hdfs(payload: dict, relative_path: str) -> str:
    webhdfs_url = _required_env("HDFS_WEBHDFS_URL").rstrip("/")
    hdfs_root = os.getenv("HDFS_RAW_ROOT", "/raw").rstrip("/")
    hdfs_path = f"{hdfs_root}{relative_path}"
    encoded_path = parse.quote(hdfs_path, safe="/=@._-")
    url = f"{webhdfs_url}/webhdfs/v1{encoded_path}"
    directory_path = hdfs_path.rsplit("/", 1)[0]
    encoded_directory = parse.quote(directory_path, safe="/=@._-")
    directory_url = f"{webhdfs_url}/webhdfs/v1{encoded_directory}"
    username = os.getenv("HDFS_USER", "airflow")

    try:
        mkdir_response = requests.put(
            directory_url,
            params={"op": "MKDIRS", "user.name": username},
            timeout=30,
        )
        mkdir_response.raise_for_status()

        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        write_response = requests.put(
            url,
            params={"op": "CREATE", "overwrite": "true", "user.name": username},
            data=body,
            headers={"Content-Type": "application/json"},
            timeout=60,
            allow_redirects=True,
        )
        write_response.raise_for_status()
    except requests.RequestException as exc:
        detail = exc.response.text[:1000] if exc.response is not None else str(exc)
        raise RuntimeError(f"Could not write Raw file to HDFS: {detail}") from exc

    namenode_rpc = os.getenv("HDFS_NAMENODE_RPC", "hdfs://namenode:9000").rstrip("/")
    return f"{namenode_rpc}{hdfs_path}"


def _write_raw(
    payload: dict | list,
    symbol: str,
    resolution: str,
    run_id: str,
    ingest_date: date,
    entity: str = "ohlc",
):
    relative_path = _raw_relative_path(symbol, resolution, run_id, ingest_date, entity)
    storage = os.getenv("RAW_STORAGE", "local").strip().lower()
    if storage == "minio":
        return _write_raw_minio(payload, relative_path)
    if storage == "hdfs":
        return _write_raw_hdfs(payload, relative_path)
    if storage == "local":
        return _write_raw_local(payload, relative_path)
    raise ValueError(f"Unsupported RAW_STORAGE={storage!r}; use 'local' or 'hdfs'")


def _record_start(conn, run_id: str, symbol: str, resolution: str, start_date: date, end_date: date, attempt: int):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl_runs
                (dag_run_id, symbol, resolution, from_date, to_date, status, attempt, started_at)
            VALUES (%s, %s, %s, %s, %s, 'running', %s, NOW())
            ON CONFLICT (dag_run_id, symbol, resolution, from_date, to_date)
            DO UPDATE SET status='running', attempt=EXCLUDED.attempt,
                          started_at=NOW(), finished_at=NULL, record_count=NULL,
                          raw_uri=NULL, error_message=NULL
            """,
            (run_id, symbol, resolution, start_date, end_date, attempt),
        )
    conn.commit()


def _record_finish(conn, run_id: str, symbol: str, resolution: str, start_date: date, end_date: date, status: str, record_count=None, raw_uri=None, error_message=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE crawl_runs
               SET status=%s, finished_at=NOW(), record_count=%s,
                   raw_uri=%s, error_message=%s
             WHERE dag_run_id=%s AND symbol=%s AND resolution=%s
               AND from_date=%s AND to_date=%s
            """,
            (status, record_count, raw_uri, error_message, run_id, symbol, resolution, start_date, end_date),
        )
    conn.commit()


def run_collection(**context):
    
    database_url = _required_env("STOCK_STATE_DATABASE_URL")
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    run_id = context.get("run_id") or "manual-" + uuid4().hex
    attempt = getattr(context.get("ti"), "try_number", 1)
    configured_symbols = conf.get("symbols") or os.getenv("DNSE_SYMBOLS", "ACB")
    if isinstance(configured_symbols, str):
        symbols = [s.strip().upper() for s in configured_symbols.split(",") if s.strip()]
    else:
        symbols = [str(s).strip().upper() for s in configured_symbols if str(s).strip()]
    # A one-off backfill can override the candle interval without changing
    # the resolution used by the scheduled daily DAG runs.
    resolution = str(conf.get("resolution") or os.getenv("DNSE_RESOLUTION", "1D")).strip()
    configured_indexes = os.getenv("DNSE_INDEX_SYMBOLS", "VNINDEX,VN30")
    index_symbols = [s.strip().upper() for s in configured_indexes.split(",") if s.strip()]
    start_date, end_date = _date_window(conf)
    start_ts = _to_epoch_seconds(start_date)
    # DNSE `to` is treated as the exclusive start of the following local day.
    end_ts = _to_epoch_seconds(end_date + timedelta(days=1))
    ingest_date = datetime.now(LOCAL_TZ).date()

    if not symbols:
        raise ValueError("DNSE_SYMBOLS contains no symbols")

    errors = []
    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS crawl_runs (
                    dag_run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    resolution TEXT NOT NULL,
                    from_date DATE NOT NULL,
                    to_date DATE NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    record_count INTEGER,
                    raw_uri TEXT,
                    error_message TEXT,
                    PRIMARY KEY (dag_run_id, symbol, resolution, from_date, to_date)
                )
                """
            )
        conn.commit()

        for symbol in symbols:
            _record_start(conn, run_id, symbol, resolution, start_date, end_date, attempt)
            try:
                response_data = _request_ohlc(symbol, resolution, start_ts, end_ts, "STOCK")
                envelope = {
                    "source": "dnse",
                    "entity": "ohlc",
                    "symbol": symbol,
                    "resolution": resolution,
                    "requested_from": start_ts,
                    "requested_to": end_ts,
                    "from_date_local": start_date.isoformat(),
                    "to_date_local_inclusive": end_date.isoformat(),
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "dag_run_id": run_id,
                    "response": response_data,
                }
                raw_uri = _write_raw(envelope, symbol, resolution, run_id, ingest_date)
                times = response_data.get("t") if isinstance(response_data, dict) else None
                if isinstance(response_data, dict):
                    count = len(times) if isinstance(times, list) else 0
                elif isinstance(response_data, list):
                    count = len(response_data)
                else:
                    count = 0
                _record_finish(conn, run_id, symbol, resolution, start_date, end_date, "success", count, raw_uri)
                print(f"Stored {symbol}: {count} candle(s), Raw={raw_uri}")
            except Exception as exc:
                _record_finish(conn, run_id, symbol, resolution, start_date, end_date, "failed", error_message=str(exc)[:4000])
                errors.append(f"{symbol}: {exc}")

        for index_symbol in index_symbols:
            status_key = f"INDEX:{index_symbol}"
            _record_start(conn, run_id, status_key, resolution, start_date, end_date, attempt)
            try:
                response_data = _request_ohlc(
                    index_symbol, resolution, start_ts, end_ts, "INDEX"
                )
                envelope = {
                    "source": "dnse",
                    "entity": "ohlc_index",
                    "symbol": index_symbol,
                    "market_type": "INDEX",
                    "resolution": resolution,
                    "requested_from": start_ts,
                    "requested_to": end_ts,
                    "from_date_local": start_date.isoformat(),
                    "to_date_local_inclusive": end_date.isoformat(),
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "dag_run_id": run_id,
                    "response": response_data,
                }
                raw_uri = _write_raw(
                    envelope, index_symbol, resolution, run_id, ingest_date, "ohlc_index"
                )
                count = _response_record_count(response_data)
                _record_finish(
                    conn, run_id, status_key, resolution, start_date, end_date,
                    "success", count, raw_uri
                )
                print(f"Stored index {index_symbol}: {count} candle(s), Raw={raw_uri}")
            except Exception as exc:
                _record_finish(
                    conn, run_id, status_key, resolution, start_date, end_date,
                    "failed", error_message=str(exc)[:4000]
                )
                errors.append(f"INDEX:{index_symbol}: {exc}")

        # The instrument endpoint is a reference-data snapshot, not a time
        # series. Record it once per DAG run under a stable synthetic key.
        instrument_key = "INSTRUMENT_MASTER"
        snapshot_resolution = "snapshot"
        _record_start(
            conn, run_id, instrument_key, snapshot_resolution,
            ingest_date, ingest_date, attempt
        )
        try:
            response_data = _request_instruments(symbols)
            envelope = {
                "source": "dnse",
                "entity": "instrument_master",
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                "snapshot_date_local": ingest_date.isoformat(),
                "dag_run_id": run_id,
                "response": response_data,
            }
            raw_uri = _write_raw(
                envelope, "ALL", snapshot_resolution, run_id, ingest_date,
                "instrument_master"
            )
            count = _response_record_count(response_data)
            _record_finish(
                conn, run_id, instrument_key, snapshot_resolution,
                ingest_date, ingest_date, "success", count, raw_uri
            )
            print(f"Stored instrument master snapshot: {count} item(s), Raw={raw_uri}")
        except Exception as exc:
            _record_finish(
                conn, run_id, instrument_key, snapshot_resolution,
                ingest_date, ingest_date, "failed", error_message=str(exc)[:4000]
            )
            errors.append(f"INSTRUMENT_MASTER: {exc}")

    if errors:
        raise RuntimeError("One or more symbol collections failed: " + " | ".join(errors))


def run_market_universe_daily(**context):
    
    database_url = _required_env("STOCK_STATE_DATABASE_URL")
    conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
    run_id = context.get("run_id") or "manual-" + uuid4().hex
    attempt = getattr(context.get("ti"), "try_number", 1)
    today = datetime.now(LOCAL_TZ).date()
    date_conf = dict(conf)
    if not date_conf.get("from_date") and not date_conf.get("to_date"):
        lookback_days = max(int(os.getenv("DNSE_MARKET_LOOKBACK_DAYS", "1")), 1)
        date_conf["from_date"] = (today - timedelta(days=lookback_days - 1)).isoformat()
        date_conf["to_date"] = today.isoformat()
    start_date, end_date = _date_window(date_conf)
    start_ts = _to_epoch_seconds(start_date)
    end_ts = _to_epoch_seconds(end_date + timedelta(days=1))
    ingest_date = today
    resolution = str(
        conf.get("resolution") or os.getenv("DNSE_MARKET_RESOLUTION", "1")
    ).strip().upper()
    resolution_aliases = {"D": "1D", "DAY": "1D", "1D": "1D", "1": "1"}
    if resolution not in resolution_aliases:
        raise ValueError("Supported market-universe resolutions are '1' (1-minute) and '1D' (daily)")
    resolution = resolution_aliases[resolution]
    ohlc_entity = "ohlc_minute" if resolution == "1" else "ohlc_daily"

    page_size = min(max(int(conf.get("page_size") or os.getenv("DNSE_INSTRUMENT_PAGE_SIZE", "100")), 1), 100)
    max_pages = max(int(conf.get("max_pages") or os.getenv("DNSE_INSTRUMENT_MAX_PAGES", "30")), 1)
    max_symbols_value = conf.get("max_symbols")
    if max_symbols_value is None:
        max_symbols_value = os.getenv("DNSE_MARKET_MAX_SYMBOLS", "0")
    max_symbols = int(max_symbols_value)
    if max_symbols < 0:
        raise ValueError("max_symbols must be zero (unlimited) or a positive integer")
    symbol_pattern = re.compile(os.getenv("DNSE_MARKET_SYMBOL_REGEX", r"^[A-Z][A-Z][A-Z]$"))
    stock_groups = {
        value.strip().upper()
        for value in os.getenv("DNSE_MARKET_STOCK_GROUP_IDS", "ST").split(",")
        if value.strip()
    }
    request_delay = max(float(os.getenv("DNSE_REQUEST_DELAY_SECONDS", "0.15")), 0.0)
    index_symbols = [
        item.strip().upper()
        for item in os.getenv("DNSE_INDEX_SYMBOLS", "VNINDEX,VN30").split(",")
        if item.strip()
    ]

    all_errors: list[str] = []
    with psycopg2.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS crawl_runs (
                    dag_run_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    resolution TEXT NOT NULL,
                    from_date DATE NOT NULL,
                    to_date DATE NOT NULL,
                    status TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    record_count INTEGER,
                    raw_uri TEXT,
                    error_message TEXT,
                    PRIMARY KEY (dag_run_id, symbol, resolution, from_date, to_date)
                )
                """
            )
        conn.commit()

        try:
            instrument_rows, pages = _request_instrument_universe(page_size, max_pages)
            for page in pages:
                page_number = page["page"]
                status_key = f"INSTRUMENT_PAGE_{page_number:04d}"
                _record_start(conn, run_id, status_key, "snapshot", ingest_date, ingest_date, attempt)
                envelope = {
                    "source": "dnse",
                    "entity": "instrument_master",
                    "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                    "snapshot_date_local": ingest_date.isoformat(),
                    "page": page_number,
                    "record_count": page["record_count"],
                    "dag_run_id": run_id,
                    "response": page["response"],
                }
                uri = _write_raw(
                    envelope, f"PAGE-{page_number:04d}", "snapshot", run_id,
                    ingest_date, "instrument_master"
                )
                _record_finish(
                    conn, run_id, status_key, "snapshot", ingest_date, ingest_date,
                    "success", page["record_count"], uri
                )

            configured_symbols = conf.get("symbols") or os.getenv("DNSE_MARKET_SYMBOLS", "").strip()
            if configured_symbols:
                if isinstance(configured_symbols, str):
                    symbols = [part.strip().upper() for part in configured_symbols.split(",") if part.strip()]
                else:
                    symbols = [str(part).strip().upper() for part in configured_symbols if str(part).strip()]
                universe_source = "dag_run_conf_or_DNSE_MARKET_SYMBOLS"
            else:
                if not instrument_rows:
                    raise RuntimeError(
                        "Instrument response was saved to Raw, but no rows matched the current parser; "
                        "inspect the Raw page and set DNSE_MARKET_SYMBOLS or update the parser"
                    )
                symbols = sorted({
                    symbol
                    for row in instrument_rows
                    if (symbol := _instrument_symbol(row))
                    and symbol_pattern.fullmatch(symbol)
                    and _instrument_security_group(row) in stock_groups
                })
                universe_source = "paginated_instrument_snapshot"
            if not symbols:
                raise RuntimeError(
                    "No symbols selected. Check instrument response keys or set DNSE_MARKET_SYMBOLS explicitly."
                )
            if max_symbols and len(symbols) > max_symbols:
                print(f"Capping market universe from {len(symbols)} to {max_symbols} symbols")
                symbols = symbols[:max_symbols]
            print(
                f"Market universe: {len(symbols)} symbols from {universe_source}; "
                f"resolution={resolution}; window {start_date}..{end_date}; "
                f"instrument rows={len(instrument_rows)}"
            )

            for symbol in symbols:
                _record_start(conn, run_id, symbol, resolution, start_date, end_date, attempt)
                try:
                    response_data = _request_ohlc(symbol, resolution, start_ts, end_ts, "STOCK")
                    envelope = {
                        "source": "dnse",
                        "entity": ohlc_entity,
                        "symbol": symbol,
                        "market_type": "STOCK",
                        "resolution": resolution,
                        "universe_source": universe_source,
                        "requested_from": start_ts,
                        "requested_to": end_ts,
                        "from_date_local": start_date.isoformat(),
                        "to_date_local_inclusive": end_date.isoformat(),
                        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                        "dag_run_id": run_id,
                        "response": response_data,
                    }
                    uri = _write_raw(envelope, symbol, resolution, run_id, ingest_date, ohlc_entity)
                    count = _response_record_count(response_data)
                    _record_finish(
                        conn, run_id, symbol, resolution, start_date, end_date,
                        "success", count, uri
                    )
                    print(f"Stored {resolution} {symbol}: {count} candle(s), Raw={uri}")
                except Exception as exc:
                    _record_finish(
                        conn, run_id, symbol, resolution, start_date, end_date,
                        "failed", error_message=str(exc)[:4000]
                    )
                    all_errors.append(f"{symbol}: {exc}")
                if request_delay:
                    time_module.sleep(request_delay)

            for index_symbol in index_symbols:
                status_key = f"INDEX:{index_symbol}"
                _record_start(conn, run_id, status_key, resolution, start_date, end_date, attempt)
                try:
                    response_data = _request_ohlc(index_symbol, resolution, start_ts, end_ts, "INDEX")
                    envelope = {
                        "source": "dnse",
                        "entity": "ohlc_index_minute" if resolution == "1" else "ohlc_index_daily",
                        "symbol": index_symbol,
                        "market_type": "INDEX",
                        "resolution": resolution,
                        "requested_from": start_ts,
                        "requested_to": end_ts,
                        "from_date_local": start_date.isoformat(),
                        "to_date_local_inclusive": end_date.isoformat(),
                        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                        "dag_run_id": run_id,
                        "response": response_data,
                    }
                    uri = _write_raw(
                        envelope, index_symbol, resolution, run_id, ingest_date,
                        "ohlc_index_minute" if resolution == "1" else "ohlc_index_daily"
                    )
                    _record_finish(
                        conn, run_id, status_key, resolution, start_date, end_date,
                        "success", _response_record_count(response_data), uri
                    )
                except Exception as exc:
                    _record_finish(
                        conn, run_id, status_key, resolution, start_date, end_date,
                        "failed", error_message=str(exc)[:4000]
                    )
                    all_errors.append(f"INDEX:{index_symbol}: {exc}")

        except Exception as exc:
            all_errors.append(f"instrument universe: {exc}")

    if all_errors:
        raise RuntimeError("Market-universe collection had failures: " + " | ".join(all_errors))
def _write_raw_minio(payload: dict, relative_path: str) -> str:
    bucket = os.environ["MINIO_BUCKET_RAW"]
    endpoint = os.environ["MINIO_ENDPOINT_URL"]

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["admin"],
        aws_secret_access_key=os.environ["namhoang16112005"],
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),
    )

    key = relative_path.lstrip("/")
    body = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"