from datetime import datetime, timedelta, timezone
from typing import List, Optional

from rp2.rp2_decimal import RP2Decimal
from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.historical_bar import HistoricalBar

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import os

class PairConverterPlugin(AbstractPairConverterPlugin):
    def __init__(self):
        super().__init__()
        self.api = CoinmarketcapAPI()

    def name(self) -> str:
        return "coinmarketcap.com"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None

        time_interval_mins = 5
        start_timestamp = timestamp - timedelta(minutes=time_interval_mins)
        utc_timestamp = start_timestamp.astimezone(timezone.utc)
        utc_timestamp_str: str = utc_timestamp.isoformat()
        
        path = 'v2/cryptocurrency/quotes/historical'
        from_id = self.api.get_id_from_asset(from_asset)
        to_id = self.api.get_id_from_asset(to_asset)
        id = str(from_id)
        convert_id = str(to_id)
        time_start = utc_timestamp_str
        count = 3

        # TODO: error handling
        api_results = self.api.api_request(path,id=id,time_start=time_start,count=count,convert_id=convert_id)

        # TODO: use actual to_asset here instead of USD
        prices = [quote.USD.price for quote in api_results.data.quotes]
        volumes = [quote.USD.volume_24h for quote in api_results.data.quotes]
        high = max(prices)
        low = min(prices)
        volume = volumes[-1] - volumes[0]

        result = HistoricalBar(
            duration=timedelta(minutes=time_interval_mins*count),
            timestamp=start_timestamp,
            open=RP2Decimal(str(prices[0])),
            high=RP2Decimal(str(high)),
            low=RP2Decimal(str(low)),
            close=RP2Decimal(str(prices[1])),
            volume=RP2Decimal(str(volume)),
        )
        
        return result

class CoinmarketcapAPI:
    is_sandbox: bool
    api_key: str
    api_base: str
    api_url: str
    id_cache: dict = {} # TODO: cache in file

    def __init__(self):
        self.is_sandbox = False
        self.api_base = 'pro-api'

        api_key = os.getenv('COINMARKETCAP_API_KEY')
        if api_key is None:
            self.api_key = 'b54bcf4d-1bca-4e8e-9a24-22ff2c3d462c'
            self.api_base = 'sandbox-api'
            self.is_sandbox = True
        else:
            self.api_key = api_key

        self.api_url = f'https://{self.api_base}.coinmarketcap.com'
    
    def api_request(self, path, *args, **kwargs):
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }

        if len(args) >= 1 and type(args[0]) is dict:
            api_parameters = args[0]
        else:
            api_parameters = kwargs

        session = Session()
        session.headers.update(headers)

        # TODO: strip extra forward slash
        full_path = f"{self.api_url}/{path}"

        # TODO: error handling
        response = session.get(full_path, params=api_parameters)
        data = json.loads(response.text)
        return data

    def get_id_from_asset(self, asset: str):
        if self.id_cache.has_key(asset):
            return self.id_cache[asset]

        data = self.api_request('v1/cryptocurrency/map', symbol=asset)
        if not data:
            raise('Unknown ID for CoinMarketCap')
        
        self.id_cache[asset] = data.id
        return data.id