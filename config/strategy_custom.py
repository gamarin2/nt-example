from nautilus_trader.config import StrategyConfig
from nautilus_trader.model import Bar, QuoteTick, TradeTick, OrderBookDeltas
from nautilus_trader.indicators.base import Indicator


class StrategyConfigCustom(StrategyConfig, frozen=True):
    indicators: list[Indicator]
    data_cls: type[Bar] | type[QuoteTick] | type[TradeTick] | type[OrderBookDeltas]
