# backtrader

Python engine for strategy backtesting and live trading. `backtrader/cerebro.py` coordinates feeds, strategies, brokers, and analyzers; `tests/` and `datas/` hold regression cases and fixtures.

## Commands

- `make test` — syncs `dev` and `plotting` extras, adds pytest dependencies, then runs pytest except `test_analyzer-timereturn.py`. It can update project dependency files.
- `make verify-types` — runs `uvx pyrefly check`.

## Practices

- Preserve strategy lifecycle and line indexing behavior when changing indicators, feeds, or order execution.
- Add focused tests with local fixture data; avoid requiring broker credentials or network access.
