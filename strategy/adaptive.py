from nautilus_trader.common.enums import LogColor
from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.correctness import PyCondition
from nautilus_trader.core.data import Data
from nautilus_trader.core.message import Event
from nautilus_trader.indicators.average.ema import ExponentialMovingAverage
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import OrderBookDeltas
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.trading.strategy import Strategy
from config.strategy_custom import StrategyConfigCustom
from .module import StrategyModule, StrategyModuleConfig
from nautilus_trader.indicators.base.indicator import Indicator
from dataclasses import dataclass
from .services import StrategyServices


class AdaptiveStrategyConfig(StrategyConfig, frozen=True):
    """
    Configuration for ``AdaptiveStrategy`` instances.
    """

    instrument_id: InstrumentId
    bar_type: BarType
    module_configs: list[StrategyModuleConfig]


class AdaptiveStrategy(Strategy):
    def __init__(self, config: AdaptiveStrategyConfig):
        super().__init__(config)

        self.strategy_services: StrategyServices | None = None

        self.modules: dict[str, StrategyModule] = {
            module_config.name: StrategyModule(module_config)
            for module_config in config.module_configs
        }

        self.indicators: list[Indicator] = []
        for module in self.modules.values():
            for indicator_config in module.config.indicator_configs:
                indicator = indicator_config.indicator_cls(indicator_config.config)
                self.indicators.append(indicator)

        self.active_modules: list[StrategyModule] = []

    def on_start(self):
        self.strategy_services = StrategyServices(
            cache=self.cache,
            portfolio=self.portfolio,
            order_factory=self.order_factory,
            log=self.log,
            clock=self.clock,
        )

        self.subscribe_bars(self.config.bar_type)
        self.log.info(f"Subscribed to {self.config.bar_type}", color=LogColor.BLUE)

        active_modules_names = self._select_modules()
        for module_name in active_modules_names:
            self.modules[module_name].activate(self.strategy_services)
            self.active_modules.append(self.modules[module_name])

    def on_reset(self) -> None:
        """
        Actions to be performed when the strategy is reset.
        """
        # Reset indicators here
        for indicator in self.indicators:
            indicator.reset()
        # TODO: possibly deactivate modules

    def on_stop(self):
        # TODO:possibly deactivate modules
        pass

    def _select_modules(self) -> list[str]:
        # TODO: select modules based on indicators
        return list(self.modules.keys())
