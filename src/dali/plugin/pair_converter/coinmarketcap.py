from datetime import datetime, timedelta, timezone
from typing import List, Optional

from rp2.rp2_decimal import RP2Decimal
from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

from os import getenv

class PairConverterPlugin(AbstractPairConverterPlugin):
    def name(self) -> str:
        return "coinmarketcap.com"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None
        utc_timestamp = timestamp.astimezone(timezone.utc)
        
        api_key = getenv('')

        return result
