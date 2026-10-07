# Raw sample: market-universe pilot

Copied from HDFS on 2026-10-07. Pilot DAG run: `manual__market_universe_pilot_20261007_1105`.

- `instrument_page_1.json`: first raw page from `/market/instruments` (100 instrument records).
- `AAA_daily.json`: daily OHLCV response for AAA, 2026-09-07 through 2026-10-07 (23 candles).
- `VNINDEX_daily.json`: daily OHLC response for VNINDEX over the same period.

Each JSON keeps the collector envelope (`source`, `entity`, `symbol`/`page`, `resolution`, request dates, fetch timestamp, run ID) and the original API response under `response`. These are copies for inspection; the authoritative Raw files remain in HDFS.

The instrument response shows a `securityGroupId` field; this page included `ST` (stocks), `FU` (futures), and `BS` (bonds). The first capture's symbol selection used a three-letter pattern only. The collector has since been tightened to require `securityGroupId=ST`; the old capped 100-symbol run should be treated as an ingestion pilot until its symbols are checked.

HDFS source paths are under `/raw/source=dnse/entity=.../ingest_date=2026-10-07/`.
