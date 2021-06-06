#!/usr/bin/env python

import asyncio
import websockets
import json
import os
import requests
import locale
from subprocess import call
from datetime import datetime, timezone
from database import create_tables
from database import insert_alert
create_tables()


async def socket():
  locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
  uri = 'wss://bstream.binance.com:9443/stream?streams=abnormaltradingnotices'

  async with websockets.connect(uri) as websocket:
    while True:
      message = await websocket.recv()
      data = json.loads(message)['data']

      if data['quotaAsset'] == 'USDT' and data['noticeType'] == 'BLOCK_TRADE':
        response = requests.get(f"https://api2.binance.com/api/v3/ticker/24hr?symbol={data['symbol']}").json()
        price_change_percent = "{:.2f}".format(float(response['priceChangePercent']))
        quote_volume =  "{:.2f}".format(float(response['quoteVolume']))
        amount = "{:.2f}".format(float(data['volume']) * float(response['lastPrice']))

        insert_alert([data['baseAsset'], amount, quote_volume, price_change_percent, data['eventType'], datetime.now(timezone.utc)])

asyncio.get_event_loop().run_until_complete(socket())
