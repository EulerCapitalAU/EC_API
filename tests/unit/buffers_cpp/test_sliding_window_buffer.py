import pytest
from EC_API.common.tick_buffers_ext import (
    SlidingWindowBuffer,
    DataExtractionPolicy,
    StatConfig,
    StatType,
)


def make_cqg_quote(timestamp: float, price: float, volume: float) -> tuple:
    return (
        0,                   # Q_CONTRACT_ID (unused)
        0,                   # Q_TYPE = QuoteTypeCQG.Trade
        float(timestamp),    # Q_UTC_TIME
        float(price),        # Q_SCALED_PRICE
        None, None,          # Q_SCALED_SOURCE_PRICE, Q_PRICE_YIELD
        float(volume),       # Q_VOL_SIGNIFICAND
        0.0,                 # Q_VOL_EXPONENT
        None, None, None, None, None, None,  # unused
        1.0,                 # Q_CORRECT_PRICE_SCALE
        None,                # Q_IS_SNAPSHOT
    )

def make_parsed_rtmd(quotes: list) -> tuple:
    return (list(quotes), None, None, None)  # [Quote, MarketValue, DetailedDOM, Corrections]


def test_get_snapshot_batch_happy_path() -> None:
    buf = SlidingWindowBuffer(
        DataExtractionPolicy.ExtractTradeTickCQG,
        10.0,  # window
        0.25,  # tick_size
        StatConfig(cal_ohlcv=True, cal_moment=True, cal_vwap=True, cal_median=False),
    )

    prices = [100, 200, 105, 114, 20, 50, 32]
    volumes = [10, 46, 4, 81, 5, 42, 23]

    quotes = [
        make_cqg_quote(float(ts), float(p), float(v))
        for ts, (p, v) in enumerate(zip(prices, volumes), start=1)
    ]
    buf.on_tick(make_parsed_rtmd(quotes))

    actual = buf.get_stat_snapshot_batch()

    open_, high, low, close, volume = actual[StatType.OHLCV]
    assert open_ == pytest.approx(100.0, abs=1e-4)
    assert high == pytest.approx(200.0, abs=1e-4)
    assert low == pytest.approx(20.0, abs=1e-4)
    assert close == pytest.approx(32.0, abs=1e-4)
    assert volume == 211

    mean, variance, skew, kurt = actual[StatType.MOMENT]
    assert mean == pytest.approx(88.7143, abs=1e-4)
    assert variance == pytest.approx(3264.7755, abs=1e-4)
    assert skew == pytest.approx(0.63964, abs=1e-4)
    assert kurt == pytest.approx(2.5299, abs=1e-4)

    (vwap,) = actual[StatType.VWAP]
    assert vwap == pytest.approx(108.00947867, abs=1e-4)

    assert StatType.MEDIAN not in actual  # cal_median=False → key absent