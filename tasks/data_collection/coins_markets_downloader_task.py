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

class CoinsMarketsDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Coins Markets downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_futures_coins_markets"

            url = "https://open-api-v3.coinglass.com/api/futures/coins-markets"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }

            params = {
                "exchanges": self.connector_name.replace("_perpetual", ""),            
            }

            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                logging.error(f"Error fetching Coins Markets data: {response.text}")
                return

            data = response.json()
            if not data.get("data"):
                logging.info("No Coins Markets data available")
                return

            for element in data["data"]:
                
                await timescale_client.insert_coins_markets(
                    table_name=table_name,
                    exchange=self.connector_name.replace("_perpetual", ""),
                    provider="coinglass",
                    symbol=element["symbol"],
                    price=element["price"],
                    marketCap=element["marketCap"],
                    oiMarketCapRatio=element["oiMarketCapRatio"],
                    avgFundingRateByOi=element["avgFundingRateByOi"],
                    avgFundingRateByVol=element["avgFundingRateByVol"],
                    open_interest=element["openInterest"],
                    openInterestAmount=element["openInterestAmount"],
                    oiVolRatio=element["oiVolRatio"],
                    volUsd=element["volUsd"],
                    volChangePercent5m=element.get("volChangePercent5m", None),
                    volChangePercent15m=element.get("volChangePercent15m", None),
                    volChangePercent30m=element.get("volChangePercent30m", None),
                    volChangePercent1h=element.get("volChangePercent1h", None),
                    volChangePercent4h=element.get("volChangePercent4h", None),
                    volChangePercent24h=element.get("volChangePercent24h", None),
                    volChange1h=element.get("volChange1h", None),
                    volChange4h=element.get("volChange4h", None),
                    volChange24h=element.get("volChange24h", None),
                    oiVolRatioChangePercent1h=element.get("oiVolRatioChangePercent1h", None),
                    oiVolRatioChangePercent4h=element.get("oiVolRatioChangePercent4h", None),
                    oiVolRatioChangePercent24h=element.get("oiVolRatioChangePercent24h", None),
                    oiChangePercent30m=element.get("oiChangePercent30m", None),
                    oiChangePercent15m=element.get("oiChangePercent15m", None),
                    oiChangePercent5m=element.get("oiChangePercent5m", None),
                    oiChangePercent24h=element.get("oiChangePercent24h", None),
                    oiChangePercent1h=element.get("oiChangePercent1h", None),
                    oiChangePercent4h=element.get("oiChangePercent4h", None),
                    oiChange24h=element.get("oiChange24h", None),
                    oiChange1h=element.get("oiChange1h", None),
                    oiChange4h=element.get("oiChange4h", None),
                    oiChange30m=element.get("oiChange30m", None),
                    oiChange15m=element.get("oiChange15m", None),
                    oiChange5m=element.get("oiChange5m", None),
                    priceChangePercent5m=element.get("priceChangePercent5m", None),
                    priceChangePercent15m=element.get("priceChangePercent15m", None),
                    priceChangePercent30m=element.get("priceChangePercent30m", None),
                    priceChangePercent1h=element.get("priceChangePercent1h", None),
                    priceChangePercent4h=element.get("priceChangePercent4h", None),
                    priceChangePercent12h=element.get("priceChangePercent12h", None),
                    priceChangePercent24h=element.get("priceChangePercent24h", None),
                    ls5m=element.get("ls5m", None),
                    longVolUsd5m=element.get("longVolUsd5m", None),
                    shortVolUsd5m=element.get("shortVolUsd5m", None),
                    ls15m=element.get("ls15m", None),
                    longVolUsd15m=element.get("longVolUsd15m", None),
                    shortVolUsd15m=element.get("shortVolUsd15m", None),
                    ls30m=element.get("ls30m", None),
                    longVolUsd30m=element.get("longVolUsd30m", None),
                    shortVolUsd30m=element.get("shortVolUsd30m", None),
                    ls1h=element.get("ls1h", None),
                    longVolUsd1h=element.get("longVolUsd1h", None),
                    shortVolUsd1h=element.get("shortVolUsd1h", None),
                    ls4h=element.get("ls4h", None),
                    longVolUsd4h=element.get("longVolUsd4h", None),
                    shortVolUsd4h=element.get("shortVolUsd4h", None),
                    ls12h=element.get("ls12h", None),
                    longVolUsd12h=element.get("longVolUsd12h", None),
                    shortVolUsd12h=element.get("shortVolUsd12h", None),
                    ls24h=element.get("ls24h", None),
                    longVolUsd24h=element.get("longVolUsd24h", None),
                    shortVolUsd24h=element.get("shortVolUsd24h", None),
                    liquidationUsd24h=element.get("liquidationUsd24h", None),
                    longLiquidationUsd24h=element.get("longLiquidationUsd24h", None),
                    shortLiquidationUsd24h=element.get("shortLiquidationUsd24h", None),
                    liquidationUsd12h=element.get("liquidationUsd12h", None),
                    longLiquidationUsd12h=element.get("longLiquidationUsd12h", None),
                    shortLiquidationUsd12h=element.get("shortLiquidationUsd12h", None),
                    liquidationUsd4h=element.get("liquidationUsd4h", None),
                    longLiquidationUsd4h=element.get("longLiquidationUsd4h", None),
                    shortLiquidationUsd4h=element.get("shortLiquidationUsd4h", None),
                    liquidationUsd1h=element.get("liquidationUsd1h", None),
                    longLiquidationUsd1h=element.get("longLiquidationUsd1h", None),
                    shortLiquidationUsd1h=element.get("shortLiquidationUsd1h", None),
                    created_date_time=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    created_at=int(datetime.now().timestamp()),
                )
                

            logging.info(f"{now} - Inserted Coins Markets data")

        except Exception as e:
            logging.exception(f"{now} - Error processing Coins Markets data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit_perpetual",
        "days_data_retention": 30,
        "coinglass_api_key": os.getenv("CG_API_KEY"),
        "timescale_config": {
            "host": os.getenv("TIMESCALE_HOST", "localhost"),
            "port": os.getenv("TIMESCALE_PORT", 5432),
            "user": "postgres",
            "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
            "database": os.getenv("TIMESCALE_DB", "timescaledb"),
        }
    }
    
    task = CoinsMarketsDownloaderTask(
        "Coins Markets Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
