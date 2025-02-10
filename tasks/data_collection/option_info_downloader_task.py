import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pandas as pd
import requests
from dotenv import load_dotenv

from core.services.timescale_client import TimescaleClient
from core.task_base import BaseTask

logging.basicConfig(level=logging.INFO)
load_dotenv()

class OptionInfoDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Option Info downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_option_info_1h"

            trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])

            for trading_pair in trading_pairs:
                symbol = trading_pair.split("-")[0]  # Only need base currency for this endpoint
                url = "https://open-api-v3.coinglass.com/api/option/info"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                params = {
                    "symbol": symbol
                }

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Option Info data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data:
                    logging.info(f"No Option Info data available for {symbol}")
                    continue

                for element in data:
                    await timescale_client.insert_option_info(
                        table_name=table_name,
                        exchange_name=element["exchangeName"],
                        open_interest=element["openInterest"],
                        rate=element["rate"],
                        h24_change=element["h24Change"],
                        exchange_logo=element["exchangeLogo"],
                        open_interest_usd=element["openInterestUsd"],
                        vol_usd=element["volUsd"],
                        h24_vol_change_percent=element["h24VolChangePercent"],
                        date_time=datetime.fromtimestamp(element["date"]).strftime("%Y-%m-%d"),
                        timestamp=element["date"],
                        created_at=int(datetime.now().timestamp())
                    )

                logging.info(f"{now} - Inserted Option Info data for {symbol}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Option Info data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit",
        "days_data_retention": 30,
        "coinglass_api_key": "579e0b5e25eb46339ca299d5d9ed891e",
        "trading_pairs": ["BTC-USDT", "ETH-USDT"],
        "timescale_config": {
            "host": "localhost",
            "port": 5434,
            "user": "postgres",
            "password": "P@ssw0rd12",
            "database": "timescaledb"
        }
    }
    
    task = OptionInfoDownloaderTask(
        "Option Info Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
