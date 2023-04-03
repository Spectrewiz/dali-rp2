import os
from datetime import datetime, timedelta, timezone
from typing import Any

from rp2.rp2_decimal import RP2Decimal

from dali.abstract_pair_converter_plugin import AssetPairAndTimestamp
from dali.cache import CACHE_DIR, load_from_cache
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar
from dali.plugin.pair_converter.coinmarketcap import PairConverterPlugin


MOCK_RESULTS: bool = False
BAR_DURATION: timedelta = timedelta(minutes=10)
BAR_TIMESTAMP: datetime = datetime(2020, 6, 1, 0, 0).replace(tzinfo=timezone.utc)
BAR_LOW: RP2Decimal = RP2Decimal("9430.01")
BAR_HIGH: RP2Decimal = RP2Decimal("9447.52")
BAR_OPEN: RP2Decimal = RP2Decimal("9445.83")
BAR_CLOSE: RP2Decimal = RP2Decimal("9435.80")
BAR_VOLUME: RP2Decimal = RP2Decimal("1")


class TestCoinmarketcap:
    # TODO: rewrite tests
    def test_historical_prices(self, mocker: Any) -> None:
        plugin: PairConverterPlugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        cache_path = os.path.join(CACHE_DIR, plugin.cache_key())
        if os.path.exists(cache_path):
            os.remove(cache_path)

        # Reinstantiate plugin now that cache is gone
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        data = plugin.get_historic_bar_from_native_source(BAR_TIMESTAMP, "BTC", "USD", "Coinbase")

        if MOCK_RESULTS:
            mocker.patch.object(plugin, "get_historic_bar_from_native_source").return_value = HistoricalBar(
                duration=BAR_DURATION,
                timestamp=BAR_TIMESTAMP,
                low=BAR_LOW,
                high=BAR_HIGH,
                open=BAR_OPEN,
                close=BAR_CLOSE,
                volume=BAR_VOLUME,
            )

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

        # Read price again, but populate plugin cache this time
        value = plugin.get_conversion_rate(BAR_TIMESTAMP, "BTC", "USD", "Coinbase")
        assert value
        assert value == BAR_HIGH

        # Save plugin cache
        plugin.save_historical_price_cache()

        # Load plugin cache and verify
        cache = load_from_cache(plugin.cache_key())
        key = AssetPairAndTimestamp(BAR_TIMESTAMP, "BTC", "USD", "Coinbase")
        assert len(cache) == 1, str(cache)
        assert key in cache
        data = cache[key]

        assert data
        assert data.timestamp == BAR_TIMESTAMP
        assert data.timestamp == BAR_TIMESTAMP
        assert data.low == BAR_LOW
        assert data.high == BAR_HIGH
        assert data.open == BAR_OPEN
        assert data.close == BAR_CLOSE
        assert data.volume == BAR_VOLUME

    # TODO: rewrite tests
    def test_missing_historical_prices(self, mocker: Any) -> None:
        plugin = PairConverterPlugin(Keyword.HISTORICAL_PRICE_HIGH.value)
        timestamp = datetime(2020, 6, 1, 0, 0)

        if MOCK_RESULTS:
            mocker.patch.object(plugin, "get_historic_bar_from_native_source").return_value = None

        data = plugin.get_historic_bar_from_native_source(timestamp, "EUR", "JPY", "Coinbase")
        assert data is None
