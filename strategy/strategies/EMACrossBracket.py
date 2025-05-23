# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import PositiveFloat
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.indicators.atr import AverageTrueRange
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.orders.list import OrderList
from nautilus_trader.trading.strategy import Strategy


# *** THIS IS A TEST STRATEGY WITH NO ALPHA ADVANTAGE WHATSOEVER. ***
# *** IT IS NOT INTENDED TO BE USED TO TRADE LIVE WITH REAL MONEY. ***


class EMACrossBracketConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``EMACrossBracket`` instances.

    Parameters
    ----------
    instrument_id : InstrumentId
        The instrument ID for the strategy.
    bar_type : BarType
        The bar type for the strategy.
    trade_size : Decimal
        The position size per trade.
    atr_period : PositiveInt, default 20
        The period for the ATR indicator.
    fast_ema_period : PositiveInt, default 10
        The fast EMA period.
    slow_ema_period : PositiveInt, default 20
        The slow EMA period.
    bracket_distance_atr : PositiveFloat, default 3.0
        The SL and TP bracket distance from entry ATR multiple.
    emulation_trigger : str, default 'NO_TRIGGER'
        The emulation trigger for submitting emulated orders.
        If ``None`` then orders will not be emulated.

    """

    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal
    historical_start_time: str
    historical_end_time: str
    atr_period: PositiveInt = 20
    fast_ema_period: PositiveInt = 10
    slow_ema_period: PositiveInt = 20
    bracket_distance_atr: PositiveFloat = 3.0
    emulation_trigger: str = "NO_TRIGGER"


class EMACrossBracket(Strategy):
    """
    A simple moving average cross example strategy.

    When the fast EMA crosses the slow EMA then enter a position at the market
    in that direction.

    Cancels all orders and closes all positions on stop.

    Parameters
    ----------
    config : EMACrossConfig
        The configuration for the instance.

    Raises
    ------
    ValueError
        If `config.fast_ema_period` is not less than `config.slow_ema_period`.

    """

    def __init__(self, config: EMACrossBracketConfig) -> None:
        PyCondition.is_true(
            config.fast_ema_period < config.slow_ema_period,
            "{config.fast_ema_period=} must be less than {config.slow_ema_period=}",
        )
        super().__init__(config)

        self.instrument: Instrument | None = None  # Initialized in on_start

        # Create the indicators for the strategy
        self.atr = AverageTrueRange(config.atr_period)
        self.fast_ema = ExponentialMovingAverage(config.fast_ema_period)
        self.slow_ema = ExponentialMovingAverage(config.slow_ema_period)

        self.is_first_bar = False

    def on_start(self) -> None:
        """
        Actions to be performed on strategy start.
        """
        print(f"start time: {self.clock.utc_now()}")
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        # Register the indicators for updating
        self.register_indicator_for_bars(self.config.bar_type, self.atr)
        self.register_indicator_for_bars(self.config.bar_type, self.fast_ema)
        self.register_indicator_for_bars(self.config.bar_type, self.slow_ema)

        # Get historical data
        historical_start_time = datetime.strptime(
            self.config.historical_start_time, "%Y-%m-%d"
        ).replace(tzinfo=timezone.utc)

        print(f"Historical start time: {historical_start_time}")
        historical_start_time_ns = historical_start_time.timestamp() * 1_000_000_000
        print(f"Historical start time (ns): {historical_start_time_ns}")

        self.request_bars(
            self.config.bar_type,
            start=historical_start_time,
            limit=1000,
        )

        # Subscribe to live data
        self.subscribe_bars(self.config.bar_type)
        self.subscribe_quote_ticks(self.config.instrument_id)

        bars = self.cache.bars(self.config.bar_type)
        print(f"INIT: Number of bars in cache: {len(bars)}")

    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        Actions to be performed when the strategy is running and receives a quote tick.

        Parameters
        ----------
        tick : QuoteTick
            The tick received.

        """
        # For debugging (must add a subscription)
        # self.log.info(repr(tick), LogColor.CYAN)

    def on_historical_data(self, data: Data) -> None:
        """
        Actions to be performed when the strategy is running and receives historical data.
        """
        print(f"[on_historical_data] Historical data received: {data}")

    def on_bar(self, bar: Bar) -> None:
        """
        Actions to be performed when the strategy is running and receives a bar.

        Parameters
        ----------
        bar : Bar
            The bar received.

        """
        if not self.is_first_bar:
            print(f"First bar timestamp init: {bar.ts_init}")
            print(f"First bar timestamp event: {bar.ts_event}")

            bars = self.cache.bars(self.config.bar_type)
            print(f"First bar in cache timestamp init: {bars[0].ts_init}")
            print(f"First bar in cache timestamp event: {bars[0].ts_event}")

            print(f"last bar in cache timestamp init: {bars[-1].ts_init}")
            print(f"last bar in cache timestamp event: {bars[-1].ts_event}")

            print(f"number of bars in cache: {len(bars)}")

            self.is_first_bar = True

        # Check if indicators ready
        if not self.indicators_initialized():
            self.log.info(
                f"Waiting for indicators to warm up [{self.cache.bar_count(self.config.bar_type)}]",
                color=LogColor.BLUE,
            )
            return  # Wait for indicators to warm up...

        if bar.is_single_price():
            # Implies no market information for this bar
            return

        # BUY LOGIC
        if self.fast_ema.value >= self.slow_ema.value:
            if self.portfolio.is_flat(self.config.instrument_id):
                self.cancel_all_orders(self.config.instrument_id)
                self.buy(bar)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self.cancel_all_orders(self.config.instrument_id)
                self.buy(bar)
        # SELL LOGIC
        elif self.fast_ema.value < self.slow_ema.value:
            if self.portfolio.is_flat(self.config.instrument_id):
                self.cancel_all_orders(self.config.instrument_id)
                self.sell(bar)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self.cancel_all_orders(self.config.instrument_id)
                self.sell(bar)

    def buy(self, last_bar: Bar) -> None:
        """
        Users bracket buy method (example).
        """
        if not self.instrument:
            self.log.error("No instrument loaded")
            return

        bracket_distance: float = self.config.bracket_distance_atr * self.atr.value
        order_list: OrderList = self.order_factory.bracket(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTD,
            expire_time=self.clock.utc_now() + timedelta(seconds=30),
            entry_price=self.instrument.make_price(last_bar.close),  # TODO
            entry_trigger_price=self.instrument.make_price(last_bar.close),  # TODO
            sl_trigger_price=self.instrument.make_price(
                last_bar.close - bracket_distance
            ),
            tp_price=self.instrument.make_price(last_bar.close + bracket_distance),
            entry_order_type=OrderType.LIMIT_IF_TOUCHED,
            emulation_trigger=TriggerType[self.config.emulation_trigger],
        )

        self.submit_order_list(order_list)

    def sell(self, last_bar: Bar) -> None:
        """
        Users bracket sell method (example).
        """
        if not self.instrument:
            self.log.error("No instrument loaded")
            return

        bracket_distance: float = self.config.bracket_distance_atr * self.atr.value
        order_list: OrderList = self.order_factory.bracket(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.SELL,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTD,
            expire_time=self.clock.utc_now() + timedelta(seconds=30),
            entry_price=self.instrument.make_price(last_bar.close),  # TODO
            entry_trigger_price=self.instrument.make_price(last_bar.close),  # TODO
            sl_trigger_price=self.instrument.make_price(
                last_bar.close + bracket_distance
            ),
            tp_price=self.instrument.make_price(last_bar.close - bracket_distance),
            entry_order_type=OrderType.LIMIT_IF_TOUCHED,
            emulation_trigger=TriggerType[self.config.emulation_trigger],
        )

        self.submit_order_list(order_list)

    def on_data(self, data: Data) -> None:
        """
        Actions to be performed when the strategy is running and receives data.

        Parameters
        ----------
        data : Data
            The data received.

        """

    def on_event(self, event: Event) -> None:
        """
        Actions to be performed when the strategy is running and receives an event.

        Parameters
        ----------
        event : Event
            The event received.

        """

    def on_stop(self) -> None:
        """
        Actions to be performed when the strategy is stopped.
        """
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

        # Unsubscribe from data
        self.unsubscribe_bars(self.config.bar_type)
        self.unsubscribe_quote_ticks(self.config.instrument_id)

    def on_reset(self) -> None:
        """
        Actions to be performed when the strategy is reset.
        """
        # Reset indicators here
        self.fast_ema.reset()
        self.slow_ema.reset()

    def on_save(self) -> dict[str, bytes]:
        """
        Actions to be performed when the strategy is saved.

        Create and return a state dictionary of values to be saved.

        Returns
        -------
        dict[str, bytes]
            The strategy state dictionary.

        """
        return {}

    def on_load(self, state: dict[str, bytes]) -> None:
        """
        Actions to be performed when the strategy is loaded.

        Saved state values will be contained in the give state dictionary.

        Parameters
        ----------
        state : dict[str, bytes]
            The strategy state dictionary.

        """

    def on_dispose(self) -> None:
        """
        Actions to be performed when the strategy is disposed.

        Cleanup any resources used by the strategy here.

        """
