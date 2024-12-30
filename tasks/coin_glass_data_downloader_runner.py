import asyncio
import logging
import os
import sys
from datetime import timedelta

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    from core.task_base import TaskOrchestrator

    from tasks.data_collection.coin_glass_data_downloader_task import CoinGlassDataDownloaderTask

    orchestrator = TaskOrchestrator()

    timescale_config = {
        "host": os.getenv("TIMESCALE_HOST", "localhost"),
        "port": os.getenv("TIMESCALE_PORT", 5432),
        "user": os.getenv("TIMESCALE_USER", "admin"),
        "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
        "database": os.getenv("TIMESCALE_DB", "timescaledb"),
    }

    trades_downloader_task = CoinGlassDataDownloaderTask(
        name="CoinGlass liquidation aggregated history",
        config={
            "timescale_config": timescale_config,
            "end_point": "global_long_short_account_ratio",
            # "end_point": "liquidation_aggregated_history",
            "days_data_retention": 7,
            "api_key": os.getenv("CG_API_KEY"),
            "trading_pairs": ["SUI-USDT","BTC-USDT", "ETH-USDT", "1000PEPE-USDT", "SOL-USDT"],
            "interval": ["1h"],
            "limit": 1000,
        },
        frequency=timedelta(minutes=30),
    )

    orchestrator.add_task(trades_downloader_task)
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())