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

class GlobalAccountRatioDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Global Account Ratio downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            

            trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])

            for trading_pair in trading_pairs:
                
                symbol = trading_pair.replace("-", "") 
                table_name=f"coinglass_{symbol}_futures_global_long_short_account_ratio_1h"
                url = "https://open-api-v3.coinglass.com/api/futures/globalLongShortAccountRatio/history"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                params = {
                    "symbol": symbol,
                    "interval": "1h",
                    "limit": 4500,
                    "exchange": self.connector_name.replace("_perpetual", ""),
                    # "startTime":1674900000
                }
                try:
                    last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name
                    )                
                    if last_timestamp:
                        params["startTime"] = int(last_timestamp)
                except:
                    params["startTime"]=1674900000
                    last_timestamp=0
                
                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Global Account Ratio data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No Global Account Ratio data available for {symbol}")
                    continue

                for element in data["data"]:
                    if(element["time"]>last_timestamp):
                        await timescale_client.insert_global_account_ratio(
                            table_name=table_name,
                            symbol=trading_pair,
                            exchange=self.connector_name.replace("_perpetual", ""),
                            provider="coinglass",
                            timestamp=element["time"],
                            timeframe="1h",
                            date_time=datetime.fromtimestamp(element["time"]).strftime("%Y-%m-%d"),
                            created_at=int(datetime.now().timestamp())
                        )

                logging.info(f"{now} - Inserted Global Account Ratio data for {symbol}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Global Account Ratio data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit",
        "days_data_retention": 30,
        "coinglass_api_key": "579e0b5e25eb46339ca299d5d9ed891e",
        "trading_pairs": ["BTCUSDT", "ETHUSDT","SOLUSDT","1000PEPEUSDT"],
        "timescale_config": {
            "host": "localhost",
            "port": 5434,
            "user": "postgres",
            "password": "P@ssw0rd12",
            "database": "timescaledb"
        }
    }
    
    task = GlobalAccountRatioDownloaderTask(
        "Global Account Ratio Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
