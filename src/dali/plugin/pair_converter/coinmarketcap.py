from datetime import datetime, timedelta, timezone
from typing import List, Optional

from rp2.rp2_decimal import RP2Decimal
from rp2.rp2_error import RP2RuntimeError
from dali.abstract_pair_converter_plugin import AbstractPairConverterPlugin
from dali.configuration import Keyword
from dali.historical_bar import HistoricalBar

from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import os

class PairConverterPlugin(AbstractPairConverterPlugin):
    def __init__(self, historical_price_type: str, fiat_priority: Optional[str] = None):
        super().__init__(historical_price_type,fiat_priority)
        self.api = CoinmarketcapAPI()

    def name(self) -> str:
        return "coinmarketcap.com"

    def cache_key(self) -> str:
        return self.name()

    def get_historic_bar_from_native_source(self, timestamp: datetime, from_asset: str, to_asset: str, exchange: str) -> Optional[HistoricalBar]:
        result: Optional[HistoricalBar] = None

        time_interval_mins = 5
        start_timestamp = timestamp
        utc_timestamp = start_timestamp.astimezone(timezone.utc)
        utc_timestamp_str: str = utc_timestamp.isoformat()

        try:
            from_id = self.api.get_id_from_asset(from_asset)
        except CoinmarketcapAPINotFoundException as exception:
            return None  # could not find asset on Coinmarketcap
        
        to_id = self.api.get_id_from_asset(to_asset)
        try:
            api_results = self.api.api_request('v2/cryptocurrency/quotes/historical',
                                               id=str(from_id),
                                               time_start=utc_timestamp_str,
                                               count=2,
                                               convert_id=str(to_id),
                                               interval=f'{time_interval_mins}m')
            status = api_results['status']
            error_code = status['error_code']
            error_message = status['error_message']
        except Exception as exception:
            raise CoinmarketcapAPIServerException('Coinmarketcap API connection error') from exception
        
        if error_code == 1006:
            return None  # cannot use API endpoint
        
        try:
            data = api_results['data']
        except:
            return None  # no quotes found

        try:
            from_id_str = str(from_id)
            to_id_str = str(to_id)

            # TODO create dataclass for quote and auto populate from constructor
            prices = [quote['quote'][to_id_str]['price'] for quote in data[from_id_str]['quotes']]
            volumes = [quote['quote'][to_id_str]['volume_24h'] for quote in data[from_id_str]['quotes']]
            high = max(prices[0:1])
            low = min(prices[0:1])
            volume = volumes[1] - volumes[0]
        except Exception as exception:
            raise CoinmarketcapAPIDataException('Coinmarketcap API unexpected data format error') from exception

        result = HistoricalBar(
            duration=timedelta(minutes=time_interval_mins*2),
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
    session: Session

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
        self.session = Session()
    
    def api_request(self, path, *args, **kwargs):
        headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': self.api_key,
        }

        if len(args) >= 1 and type(args[0]) is dict:
            api_parameters = args[0]
        else:
            api_parameters = kwargs

        self.session.headers.update(headers)
        full_path = f"{self.api_url.strip('/')}/{path.strip('/')}"

        response = self.session.get(full_path, params=api_parameters)
        data = json.loads(response.text)
        return data

    def get_id_from_asset(self, asset: str):
        if asset in self.id_cache:
            return self.id_cache[asset]

        try:
            results = self.api_request('/v1/cryptocurrency/map', symbol=asset)
            status = results['status']
            error_code = status['error_code']
            error_message = status['error_message']
        except Exception as exception:
            raise CoinmarketcapAPIServerException(f'Error connecting to Coinmarketcap server') from exception
        
        NONE_FOUND_ERROR_MESSAGE = f'No CoinMarketCap ID found for crypto {asset}'
        if error_code == 400 and 'Invalid value for "symbol"' in error_message:
            raise CoinmarketcapAPINotFoundException(NONE_FOUND_ERROR_MESSAGE)
        
        try:
            data = results['data']
        except Exception as exception:
            raise CoinmarketcapAPINotFoundException(NONE_FOUND_ERROR_MESSAGE)

        try:
            if type(data) is list:
                id_info_list = data
            elif type(data) is dict:
                for name, info_list in data.items():
                    if name.casefold() == asset.casefold():
                        id_info_list = info_list
                        break

            id_info_list.sort(key=lambda info: info['rank'])
            id = None
            for id_info in id_info_list:
                if self.is_sandbox or id_info['symbol'].casefold() == asset.casefold():
                    id = id_info['id']
                    break
        except Exception as exception:
            raise CoinmarketcapAPIDataException(f'Coinmarketcap API unexpected results, data error') from exception

        if id is None:
            raise CoinmarketcapAPINotFoundException(NONE_FOUND_ERROR_MESSAGE)
        
        self.id_cache[asset] = id
        return id

class CoinmarketcapAPIServerException(RP2RuntimeError):
    pass

class CoinmarketcapAPIDataException(RP2RuntimeError):
    pass

class CoinmarketcapAPINotFoundException(RP2RuntimeError):
    pass
