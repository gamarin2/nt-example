from nautilus_trader.cache.cache import Cache
from nautilus_trader.portfolio.base import Portfolio
from nautilus_trader.common.factories import OrderFactory
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import Clock
from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyServices:
    cache: Cache
    portfolio: Portfolio
    order_factory: OrderFactory
    log: Logger
    clock: Clock
