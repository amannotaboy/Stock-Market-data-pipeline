# 100-symbol 1-minute backfill sample

- DAG run: `manual__market_universe_100_1m_20260708_20261007`
- Requested dates: 2026-07-08 through 2026-10-07
- Selected: first 100 alphabetically among the discovered 3-letter instruments in group `ST`; this is a pipeline pilot, not a liquidity-ranked market sample.
- `ACB_1m.json`: one raw response copied from HDFS for inspection.
- `symbol_candle_counts.csv`: record count reported per selected stock symbol.
- Full raw data remains in HDFS under `/raw/source=dnse/entity=ohlc_minute/ingest_date=2026-10-07/`.
