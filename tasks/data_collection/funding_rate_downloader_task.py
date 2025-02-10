import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
import os
import pandas as pd
import requests
from dotenv import load_dotenv

from core.services.timescale_client import TimescaleClient
from core.task_base import BaseTask

logging.basicConfig(level=logging.INFO)
load_dotenv()


class FundingRateDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.quote_asset = config.get("quote_asset", "USDT")
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(
            f"{now} - Starting funding rate downloader for {self.connector_name}"
        )

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])
        
        for trading_pair in trading_pairs:
            try:
                symbol = trading_pair.replace("-", "")
                
                table_name = f"coinglass_{trading_pair.replace('-','')}_funding_rates_ohlc_1h"
                
                params = {
                    "exchange": self.connector_name.replace("_perpetual", ""),
                    "symbol": symbol,
                    "interval": "1h",
                    "limit":4500,
                    # "startTime":"1683111600" #PEPE
                    # # "startTime":1672531200
                }       
                try:
                    last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name,                    
                    )
                    if last_timestamp:                    
                        params["startTime"] = int(last_timestamp)
                except:
                    params["startTime"] = 1672531200
                    last_timestamp=0
                    if(symbol=="PEPE"):
                        params["startTime"] = 1683111600
                        last_timestamp=0             

                url = f"https://open-api-v3.coinglass.com/api/futures/fundingRate/ohlc-history"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching data for {trading_pair}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No funding rate data for {trading_pair}")
                    continue
                
                for element in data["data"]:
                    if(float(element["t"])>last_timestamp):
                        await timescale_client.insert_funding_rates(
                            table_name=table_name,
                            symbol=symbol,
                            provider="coinglass",
                            exchange=self.connector_name.replace("_perpetual", ""),
                            open=float(element["o"]),
                            high=float(element["h"]),
                            low=float(element["l"]),
                            close=float(element["c"]),
                            timestamp=float(element["t"]),
                            date_time=datetime.fromtimestamp(float(element["t"])).strftime("%Y-%m-%d"),
                            created_at=int(datetime.now().timestamp()),
                        )

                logging.info(f"{now} - Inserted funding rates for {trading_pair}")
                await asyncio.sleep(1)  # Rate limiting

            except Exception as e:
                logging.exception(f"{now} - Error processing {trading_pair}: {e}")
                continue

        await timescale_client.close()

    @staticmethod
    def now():
        return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f UTC')


if __name__ == "__main__":
    config = {
        "connector_name": "Bybit_perpetual",
        "quote_asset": "USDT",
        "days_data_retention": 30,
        "coinglass_api_key": os.getenv("CG_API_KEY"),
        "trading_pairs": ["BTCUSDT","ETHUSDT","SOLUSDT","1000PEPEUSDT"],
        "timescale_config": {
            "host": os.getenv("TIMESCALE_HOST", "localhost"),
            "port": os.getenv("TIMESCALE_PORT", 5432),
            "user": "postgres",
            "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
            "database": os.getenv("TIMESCALE_DB", "timescaledb"),
        }
    }
    
    task = FundingRateDownloaderTask(
        "Funding Rate Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute()) 