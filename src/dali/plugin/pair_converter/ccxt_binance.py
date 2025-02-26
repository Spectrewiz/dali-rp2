# Copyright 2023 Neal Chambers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional

from dali.plugin.pair_converter.ccxt import (
    PairConverterPlugin as CcxtPairConverterPlugin,
)


class PairConverterPlugin(CcxtPairConverterPlugin):
    def __init__(
        self,
        historical_price_type: str,
        fiat_priority: Optional[str] = None,
    ) -> None:

        super().__init__(
            historical_price_type=historical_price_type,
            default_exchange="Binance.com",
            fiat_priority=fiat_priority,
            exchange_locked=True,
        )
