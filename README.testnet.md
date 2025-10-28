# Binance USDS Futures Testnet Guide

This document outlines how to run the Binance USDⓈ-M futures adapters against the official Binance Futures testnet.

## Prerequisites
- Export valid API credentials for the testnet:
  ```bash
  export BINANCE_TESTNET_KEY=your_key
  export BINANCE_TESTNET_SECRET=your_secret
  ```
- Optional: set `DEBUG=1` to enable debug logging for payload redaction checks.

## Testing
- Integration tests (live testnet calls):
  ```bash
  $ BINANCE_TESTNET_KEY=... BINANCE_TESTNET_SECRET=... pytest -m integration -v
  ```
- Unit tests with debug payloads:
  ```bash
  $ DEBUG=1 pytest -m unit -s
  ```

## Smoke Test
Run the convenience script to exercise a limit order lifecycle:
```bash
$ python -m app.scripts.smoke_order
```

The script will place a limit order using the configured credentials, fetch the status, and cancel it to keep the account clean.
