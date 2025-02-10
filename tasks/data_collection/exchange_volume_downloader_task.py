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

class ExchangeVolumeDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Exchange Volume downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            

            trading_pairs = self.config.get("trading_pairs", ["BTC"])

            for trading_pair in trading_pairs:
                table_name=f"coinglass_{trading_pair.replace('-','')}_option_exchange_vol_history_1d"
                symbol = trading_pair.split("-")[0]  # Only need base currency for this endpoint
                url = "https://open-api-v3.coinglass.com/api/option/exchange-vol-history"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                params = {
                    "symbol": symbol,
                    "currency": "USD"
                }

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Exchange Volume data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No Exchange Volume data available for {symbol}")
                    continue

                try:
                    last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name                    
                    )                
                except:
                    last_timestamp=0
                for index in range(len(data['data']['dateList'])):    
                        if(data["data"]["dateList"][index]>last_timestamp):
                            await timescale_client.insert_exchange_volume(
                                table_name=table_name,
                                symbol=trading_pair,
                                provider="coinglass",
                                priceList=data["data"]["priceList"][index],
                                Deribit=data["data"]["dataMap"]["Deribit"][index] if "Deribit" in data["data"]["dataMap"] else None,
                                CME=data["data"]["dataMap"]["CME"][index] if "CME" in data["data"]["dataMap"] else None,
                                OKX=data["data"]["dataMap"]["OKX"][index] if "OKX" in data["data"]["dataMap"] else None,
                                Binance=data["data"]["dataMap"]["Binance"][index] if "Binance" in data["data"]["dataMap"] else None,
                                Bybit=data["data"]["dataMap"]["Bybit"][index] if "Bybit" in data["data"]["dataMap"] else None,                        
                                timestamp=data["data"]["dateList"][index],
                                date_time=datetime.fromtimestamp(data["data"]["dateList"][index]/1000).strftime("%Y-%m-%d"),
                                created_at=int(datetime.now().timestamp())
                            )

                logging.info(f"{now} - Inserted Exchange Volume data for {symbol}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Exchange Volume data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit",
        "days_data_retention": 30,
        "coinglass_api_key": os.getenv("CG_API_KEY"),
        "trading_pairs": ["BTC","ETH","SOL","1000PEPE"],
        "timescale_config": {
            "host": os.getenv("TIMESCALE_HOST", "localhost"),
            "port": os.getenv("TIMESCALE_PORT", 5432),
            "user": "postgres",
            "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
            "database": os.getenv("TIMESCALE_DB", "timescaledb"),
        }
    }
    
    task = ExchangeVolumeDownloaderTask(
        "Exchange Volume Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
