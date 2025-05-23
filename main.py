import asyncio
from pathlib import Path
import pandas as pd
from pprint import pprint
from datetime import datetime

from nautilus_trader.backtest.config import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestRunConfig,
    BacktestVenueConfig,
)
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.config import LoggingConfig, RiskEngineConfig
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.model.enums import (
    PriceType,
    BarAggregation,
    AggregationSource,
)
from nautilus_trader.model.data import Bar, BarType, BarSpecification
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.persistence.config import DataCatalogConfig
from utils.binance_data import get_combined_dataframe
from nautilus_trader.config import DataEngineConfig


def create_bars_from_df(
    df: pd.DataFrame,
    instrument: Instrument,
    bar_type: BarType,
) -> list[Bar]:
    # Read CSV and convert open_time to datetime index
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df.set_index("timestamp", inplace=True)

    # Select and rename required columns
    df = df[["open", "high", "low", "close", "volume"]]

    wrangler = BarDataWrangler(bar_type=bar_type, instrument=instrument)
    bars = wrangler.process(data=df)

    return bars


async def run_backtest(
    quote_symbol: str,
    base_symbol: str,
    start_date: str,
    end_date: str,
    backtest_start_date: str | None = None,
    interval: str = "1h",
):
    # Validate backtest_start_date if provided
    if backtest_start_date is not None:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        backtest_start = datetime.strptime(backtest_start_date, "%Y-%m-%d").date()

        if not (start <= backtest_start <= end):
            raise ValueError(
                f"backtest_start_date {backtest_start_date} must be between "
                f"start_date {start_date} and end_date {end_date}"
            )
    # Create data directory if it doesn't exist
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    symbol = f"{quote_symbol}{base_symbol}"

    # Set up the catalog for storing data
    catalog_path = Path("catalog")
    if catalog_path.exists():
        import shutil

        shutil.rmtree(catalog_path)
    catalog_path.mkdir()

    catalog = ParquetDataCatalog(str(catalog_path))

    # Instruments define the trading pair on a given exchange
    # Include info like symbol, size precision, price precision, maker/taker fee, etc.
    instrument = TestInstrumentProvider.btcusdt_perp_binance()

    # BarSpecification defines the time step and aggregation of the bar data
    # Step is the time step of the bar (e.g. 1 hour)
    # Aggregation is the type of aggregation (e.g. last price)
    # PriceType is the type of price (e.g. last, mid, bid, ask)
    bar_spec = BarSpecification(
        step=1,
        aggregation=BarAggregation.HOUR,
        price_type=PriceType.LAST,
    )

    # BarType is the type of bar data
    # InstrumentId is the instrument id (e.g. BTCUSDT-PERP)
    # BarSpec is the bar specification
    # AggregationSource is the source of the aggregation (EXTERNAL or INTERNAL)
    bar_type = BarType(
        instrument_id=instrument.id,
        bar_spec=bar_spec,
        aggregation_source=AggregationSource.EXTERNAL,
    )

    combined_df = await get_combined_dataframe(
        symbol=symbol,
        interval=interval,
        start_date=start_date,
        end_date=end_date,
    )

    # Create bar data
    bars = create_bars_from_df(
        df=combined_df,
        instrument=instrument,
        bar_type=bar_type,
    )

    catalog.write_data([instrument])
    catalog.write_data(bars)

    # Print first 5 bar timestamps
    print("\nFirst 5 bar timestamps:")
    for i, bar in enumerate(catalog.bars(bar_types=[str(bar_type)])[:5]):
        print(f"Bar {i + 1}: {bar.ts_init}")

    # Configure the backtest data
    # Represents the data configuration for one specific backtest run.
    # Note: It's possible to use instrument_ids and bar_types instead of instrument_id and bar_spec
    data_config = BacktestDataConfig(
        catalog_path=catalog.path,
        data_cls=Bar,
        instrument_id=instrument.id,
        bar_spec=bar_spec,
    )

    catalogs = [
        DataCatalogConfig(
            path=catalog.path,
        ),
    ]

    default_leverage: float = 10.0

    # Configure the backtest venue
    # Add book type if dealing with order book data (won't work with bars only)
    venue_config = BacktestVenueConfig(
        name="BINANCE",
        oms_type="HEDGING",  # HEDGING allows for both long and short positions on a given contract, while NETTING only allows for one position per contract
        account_type="MARGIN",
        base_currency="USDT",  # None for multi-currency accounts
        default_leverage=default_leverage,
        starting_balances=["10000000 USDT"],
        reject_stop_orders=False,  # Stop orders are not rejected on submission if trigger price is in the market
        bar_execution=True,
        bar_adaptive_high_low_ordering=True,
    )

    # Define strategies
    strategies = [
        ImportableStrategyConfig(
            strategy_path="strategy.strategies.EMACrossBracket:EMACrossBracket",
            config_path="strategy.strategies.EMACrossBracket:EMACrossBracketConfig",
            config={
                "instrument_id": instrument.id,
                "bar_type": bar_type,
                "trade_size": 0.01,
                "atr_period": 20,
                "fast_ema_period": 10,
                "slow_ema_period": 20,
                "bracket_distance_atr": 3.0,
                "historical_start_time": start_date,
                "historical_end_time": backtest_start_date,
            },
        ),
    ]

    data_engine = DataEngineConfig(
        time_bars_origins={
            BarAggregation.HOUR: pd.Timedelta(seconds=0),
        },
    )

    logging = LoggingConfig(
        bypass_logging=False,
        log_colors=True,
        log_level="ERROR",
        log_level_file="DEBUG",
        log_directory="logs",
        log_file_format=None,  # "json" or None
        log_file_name="backtest",
        clear_log_file=True,
        print_config=False,
        use_pyo3=False,
    )

    # Configure the backtest engine
    # trader_id : TraderId
    # The trader ID for the node (must be a name and ID tag separated by a hyphen).
    # log_level : str, default "INFO"
    # The stdout log level for the node.
    # loop_debug : bool, default False
    # If the asyncio event loop should be in debug mode.
    # cache : CacheConfig, optional
    # The cache configuration.
    # data_engine : DataEngineConfig, optional
    # The live data engine configuration.
    # risk_engine : RiskEngineConfig, optional
    # The live risk engine configuration.
    # exec_engine : ExecEngineConfig, optional
    # The live execution engine configuration.
    # streaming : StreamingConfig, optional
    # The configuration for streaming to feather files.
    # strategies : list[ImportableStrategyConfig]
    # The strategy configurations for the kernel.
    # actors : list[ImportableActorConfig]
    # The actor configurations for the kernel.
    # exec_algorithms : list[ImportableExecAlgorithmConfig]
    # The execution algorithm configurations for the kernel.
    # controller : ImportableControllerConfig, optional
    # The trader controller for the kernel.
    # load_state : bool, default True
    # If trading strategy state should be loaded from the database on start.
    # save_state : bool, default True
    # If trading strategy state should be saved to the database on stop.
    # bypass_logging : bool, default False
    # If logging should be bypassed.
    # run_analysis : bool, default True
    # If post backtest performance analysis should be run.
    backtest_engine_config = BacktestEngineConfig(
        strategies=strategies,
        logging=logging,
        risk_engine=RiskEngineConfig(
            bypass=True,  # Example of bypassing pre-trade risk checks for backtests
        ),
        catalogs=catalogs,
        data_engine=data_engine,
    )

    # Create backtest configuration
    config = BacktestRunConfig(
        engine=backtest_engine_config,
        data=[data_config],
        venues=[venue_config],
        start=backtest_start_date,
        end=end_date,
    )

    # Create backtest node
    node = BacktestNode(configs=[config])

    # Run the backtest
    print(f"Running backtest for {symbol} from {backtest_start_date} to {end_date}...")
    results = node.run()

    # Generate reports
    print("Generating reports...")

    engine = node.get_engine(config.id)
    if engine:
        # Generate and save order fills report if available as DataFrame
        fills_report = None
        if hasattr(engine.trader, "generate_order_fills_report"):
            fills_report = engine.trader.generate_order_fills_report()
            if isinstance(fills_report, pd.DataFrame):
                fills_report.to_csv("order_fills_report.csv")
        # Generate and save positions report if available as DataFrame
        positions_report = None
        if hasattr(engine.trader, "generate_positions_report"):
            positions_report = engine.trader.generate_positions_report()
            if isinstance(positions_report, pd.DataFrame):
                positions_report.to_csv("positions_report.csv")

    print("Backtest complete!")

    return results


def main():
    # Run the backtest
    results = asyncio.run(
        run_backtest(
            base_symbol="USDT",
            quote_symbol="BTC",
            interval="1h",
            start_date="2024-01-01",
            end_date="2024-12-01",
            backtest_start_date="2024-01-03",
        )
    )

    pprint(results)


if __name__ == "__main__":
    main()
