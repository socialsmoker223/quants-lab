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

class AggregatedOrderbookDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Aggregated Orderbook downloader")

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

                table_name = f"coinglass_{trading_pair.replace('-','')}_spot_aggregated_orderbook_bid_ask_1h"
                symbol = trading_pair.split("-")[0]
                url = "https://open-api-v3.coinglass.com/api/futures/orderbook/aggregated-history"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                params = {
                    "symbol": symbol,
                    "interval": "1h",
                    "limit": 4500,
                    "exchanges": self.connector_name.replace("_perpetual", ""),
                    # "startTime":1706118000
                }
                
                try:
                    last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name                    
                    )
                    if last_timestamp:
                        params["startTime"] = int(last_timestamp)
                except:
                    last_timestamp=0
                    params["startTime"]=1713952800
                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Aggregated Orderbook data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No Aggregated Orderbook data available for {symbol}")
                    continue

                for element in data["data"]:
                    if(element["t"]>last_timestamp):
                        await timescale_client.insert_aggregated_orderbook(
                            table_name=table_name,
                            symbol=trading_pair,
                            exchange=self.connector_name.replace("_perpetual", ""),
                            provider="coinglass",
                            timestamp=element["time"],
                            timeframe="1h",
                            date_time=datetime.fromtimestamp(element["time"]).strftime("%Y-%m-%d"),
                            created_at=int(datetime.now().timestamp()),
                            bids_usd=element["bidsUsd"],
                            bids_amount=element["bidsAmount"],
                            asks_usd=element["asksUsd"],
                            asks_amount=element["asksAmount"]
                        )

                logging.info(f"{now} - Inserted Aggregated Orderbook data for {trading_pair}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Aggregated Orderbook data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit_perpetual",
        "days_data_retention": 30,
        "coinglass_api_key": os.getenv("CG_API_KEY"),
        "trading_pairs": ["BTC", "ETH","1000PEPE","SOL"],
        "timescale_config": {
            "host": os.getenv("TIMESCALE_HOST", "localhost"),
            "port": os.getenv("TIMESCALE_PORT", 5432),
            "user": "postgres",
            "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
            "database": os.getenv("TIMESCALE_DB", "timescaledb"),
        }
    }
    
    task = AggregatedOrderbookDownloaderTask(
        "Aggregated Orderbook Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
