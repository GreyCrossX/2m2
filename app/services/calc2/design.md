v# Calc Service - Clean Architecture Refactor

## 1. Input/Output Specification

### 📥 INPUTS (What it consumes)

| Stream | Format | Frequency | Example Key |
|--------|--------|-----------|-------------|
| Market 2m candles | Redis Stream | Every 2 minutes | `stream:market|{BTCUSDT:2m}` |

**Input Message Schema:**
```json
{
  "ts": "1760466119999",
  "sym": "BTCUSDT",
  "tf": "2m",
  "open": "121500.50",
  "high": "121600.00",
  "low": "121400.00",
  "close": "121550.00",
  "volume": "1234.567",
  "trades": "5678",
  "color": "green"
}
```

---

### 📤 OUTPUTS (Expected outcomes)

#### Output 1: Indicator Stream
**Key:** `stream:ind|{SYMBOL:2m}`
**Purpose:** Enriched market data + calculated indicators
**Frequency:** Every 2m candle (real-time)

**Schema:**
```json
{
  "v": "1",
  "sym": "BTCUSDT",
  "tf": "2m",
  "ts": "1760466119999",
  "open": "121500.50",
  "high": "121600.00",
  "low": "121400.00",
  "close": "121550.00",
  "color": "green",
  "ma20": "121450.25",
  "ma200": "120000.00",
  "regime": "long",
  "ind_ts": "1760465999999",
  "ind_high": "121600.00",
  "ind_low": "121400.00"
}
```

#### Output 2: Indicator Snapshot (Redis Hash)
**Key:** `snap:ind|{SYMBOL:2m}`
**Purpose:** Latest state for quick queries
**Schema:** Same as indicator stream

#### Output 3: Signal Stream
**Key:** `stream:signal|{SYMBOL:2m}`
**Purpose:** Trading signals (ARM/DISARM)
**Frequency:** On regime changes only

**ARM Signal Schema:**
```json
{
  "v": "1",
  "type": "arm",
  "side": "long",
  "sym": "BTCUSDT",
  "tf": "2m",
  "ts": "1760466119999",
  "ind_ts": "1760465999999",
  "ind_high": "121600.00",
  "ind_low": "121400.00",
  "trigger": "121600.01",
  "stop": "121399.99"
}
```

**DISARM Signal Schema:**
```json
{
  "v": "1",
  "type": "disarm",
  "prev_side": "long",
  "sym": "BTCUSDT",
  "tf": "2m",
  "ts": "1760466239999",
  "reason": "regime:long->neutral"
}
```

---

## 2. Async Architecture Design

```
┌─────────────────────────────────────────────────────────┐
│                     main()                              │
│  • Load config                                          │
│  • Get symbols from settings                            │
│  • Create SymbolProcessor for each symbol               │
│  • Launch all processors concurrently                   │
└─────────────────────────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
          ▼               ▼               ▼
    ┌─────────┐     ┌─────────┐     ┌─────────┐
    │ BTCUSDT │     │ ETHUSDT │     │ Symbol3 │
    │Processor│     │Processor│     │Processor│
    └─────────┘     └─────────┘     └─────────┘
          │
          ├──► StreamConsumer (reads market stream)
          │         │
          │         ├──► IndicatorCalculator (MA20, MA200)
          │         │
          │         ├──► RegimeDetector (long/short/neutral)
          │         │
          │         ├──► IndicatorTracker (tracks red/green candles)
          │         │
          │         └──► SignalGenerator (ARM/DISARM logic)
          │
          └──► StreamPublisher (writes indicators + signals)
```

### Key Principles:
1. **One processor per symbol** - fully isolated, no shared state
2. **Single responsibility** - each class does ONE thing
3. **Async all the way** - no blocking operations
4. **Error isolation** - one symbol crash doesn't affect others
5. **Clean dependencies** - dependency injection throughout

---

## 3. Component Structure

```
app/services/calc/
├── __init__.py
├── main.py                    # Entry point + orchestration *
├── config.py                  # Configuration dataclass *
├── models.py                  # Data models (Candle, Signal, etc.) *
├── processors/
│   ├── __init__.py
│   └── symbol_processor.py    # Main per-symbol processor *
├── indicators/
│   ├── __init__.py
│   ├── sma.py                 # Simple Moving Average *
│   ├── calculator.py          # Indicator calculation coordinator *
│   └── tracker.py             # Track indicator candles (red/green) *
├── regime/
│   ├── __init__.py
│   └── detector.py            # Regime detection logic *
├── signals/
│   ├── __init__.py
│   └── generator.py           # Signal generation (ARM/DISARM) *
├── streams/
│   ├── __init__.py
│   ├── consumer.py            # Read from Redis streams *
│   └── publisher.py           # Write to Redis streams *
└── utils/
    ├── __init__.py
    ├── keys.py                # Redis key generation *
    └── logging.py             # Logging utilities *
```

---

## 4. Implementation Plan

### Phase 1: Core Models
- `models.py` - All data classes (Candle, Signal, IndicatorState)
- `config.py` - Configuration management

### Phase 2: Building Blocks
- `indicators/sma.py` - MA calculation (reuse existing)
- `indicators/calculator.py` - Coordinate all indicators
- `regime/detector.py` - Regime detection
- `indicators/tracker.py` - Track indicator candles

### Phase 3: I/O Layer
- `streams/consumer.py` - Redis stream consumer
- `streams/publisher.py` - Redis stream/hash publisher
- `utils/keys.py` - Key generation helpers

### Phase 4: Business Logic
- `signals/generator.py` - Signal generation logic
- `processors/symbol_processor.py` - Main orchestrator

### Phase 5: Service Entry
- `main.py` - Service startup and task management

---

## 5. Key Features

✅ **Multi-symbol support** - Each symbol runs independently
✅ **Crash recovery** - Automatic restart with exponential backoff
✅ **Resume capability** - Continue from last processed timestamp
✅ **Clean separation** - Each component has one responsibility
✅ **Type safety** - Full type hints throughout
✅ **Testable** - Easy to unit test each component
✅ **Observable** - Comprehensive logging at each layer
✅ **Configurable** - All parameters via environment variables

---

## 6. Execution Flow

```
1. main() starts
2. Load config from environment
3. Get symbols: ['BTCUSDT', 'ETHUSDT']
4. For each symbol:
   a. Create SymbolProcessor instance
   b. Launch as asyncio task
5. SymbolProcessor.run():
   a. Initialize components:
      - StreamConsumer(symbol)
      - IndicatorCalculator()
      - RegimeDetector()
      - SignalGenerator()
      - StreamPublisher(symbol)
   b. Wait for input stream
   c. Bootstrap: read historical data
   d. Live mode: consume real-time
   e. For each candle:
      i.   Calculate indicators (MA20, MA200)
      ii.  Detect regime (long/short/neutral)
      iii. Track indicator candles
      iv.  Generate signals if regime changed
      v.   Publish indicators to stream
      vi.  Publish signals if any
6. Tasks run forever (or until error/cancel)
```

---

## 7. Error Handling Strategy

- **Stream read errors**: Retry with exponential backoff
- **Redis write errors**: Log and continue (don't stop processing)
- **Calculation errors**: Log and skip candle
- **Symbol processor crash**: Auto-restart with backoff
- **Fatal errors**: Log stack trace and exit

---

## Next Steps

I'll now implement all components with:
1. Clean, focused classes (50-100 lines each)
2. Full type hints
3. Comprehensive error handling
4. Clear logging
5. Zero shared state between symbols