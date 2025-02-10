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

class TwoHundredWeekMAHeatmapDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Two Hundred Week MA Heatmap downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_tow_hundred_week_moving_avg_heatmap_1h"

            url = "https://open-api-v3.coinglass.com/api/index/tow-hundred-week-moving-avg-heatmap"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Error fetching Two Hundred Week MA Heatmap data: {response.text}")
                return

            data = response.json()
            if not data.get("data"):
                logging.info("No Two Hundred Week MA Heatmap data available")
                return

            for element in data["data"]:
                await timescale_client.insert_two_hundred_week_ma_heatmap(
                    table_name=table_name,
                    provider="coinglass",
                    buy_qty=element["buyQty"],
                    price=element["price"],
                    ma1440=element["mA1440"],
                    ma1440ip=element["mA1440IP"],
                    sell_qty=element["sellQty"],
                    provider_date_time=datetime.fromtimestamp(element["date"]).strftime("%Y-%m-%d"),
                    provider_timestamp=element["date"],
                    created_at=int(datetime.now().timestamp())
                )

            logging.info(f"{now} - Inserted Two Hundred Week MA Heatmap data")

        except Exception as e:
            logging.exception(f"{now} - Error processing Two Hundred Week MA Heatmap data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit",
        "days_data_retention": 30,
        "coinglass_api_key": "579e0b5e25eb46339ca299d5d9ed891e",
        "timescale_config": {
            "host": "localhost",
            "port": 5434,
            "user": "postgres",
            "password": "P@ssw0rd12",
            "database": "timescaledb"
        }
    }
    
    task = TwoHundredWeekMAHeatmapDownloaderTask(
        "Two Hundred Week MA Heatmap Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
