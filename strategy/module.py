from nautilus_trader.indicators.base.indicator import Indicator
from nautilus_trader.model.data import Bar, QuoteTick, TradeTick, OrderBookDeltas
from .services import StrategyServices


class IndicatorConfig:
    indicator_cls: type[Indicator]
    config: dict
    required_data_cls: set[type[Bar] | type[QuoteTick] | type[TradeTick]]


class StrategyModuleConfig:
    name: str
    indicator_configs: list[IndicatorConfig]
    additional_data_cls: set[
        type[Bar] | type[QuoteTick] | type[TradeTick] | type[OrderBookDeltas]
    ]


class StrategyModule:
    def __init__(self, config: StrategyModuleConfig):
        self.config = config
        self.services: StrategyServices | None = None

    def activate(self, services: StrategyServices):
        self.services = services
        self._activate()

    def deactivate(self):
        self.services = None
        self._deactivate()

    def process_bar(self, bar: Bar, indicators: list[Indicator]):
        if self.services is None:
            raise ValueError("Strategy services not set")
        self._process_bar(bar, indicators)

    def process_quote_tick(self, quote_tick: QuoteTick, indicators: list[Indicator]):
        if self.services is None:
            raise ValueError("Strategy services not set")
        self._process_quote_tick(quote_tick, indicators)

    def process_trade_tick(self, trade_tick: TradeTick, indicators: list[Indicator]):
        if self.services is None:
            raise ValueError("Strategy services not set")
        self._process_trade_tick(trade_tick, indicators)

    def _process_bar(self, bar: Bar, indicators: list[Indicator]):
        pass

    def _process_quote_tick(self, quote_tick: QuoteTick, indicators: list[Indicator]):
        pass

    def _process_trade_tick(self, trade_tick: TradeTick, indicators: list[Indicator]):
        pass

    def _activate(self):
        pass

    def _deactivate(self):
        pass
