# Full stock universe 1-minute backfill sample

- Airflow run: `manual__market_universe_all_1m_90d_20261007`
- Requested window: 2026-07-08 through 2026-10-07
- Selected: all 1,399 three-letter instruments in DNSE group `ST` returned by the current instrument pagination/filter.
- Result: 2,416,004 stock candles; 107 symbols returned no candles. VNINDEX and VN30 were also stored separately.
- `ACB_1m.json`: one raw HDFS file copied locally for inspection.
- `symbol_candle_counts.csv`: candle count per stock symbol.
- Full Raw remains in HDFS under `/raw/source=dnse/entity=ohlc_minute/ingest_date=2026-10-07/`.
- The 1,399-symbol universe is the result of the current parser/filter; it is not verified as the full set of active, currently traded listings.
