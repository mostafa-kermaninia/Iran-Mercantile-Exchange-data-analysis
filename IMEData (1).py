import asyncio
import aiohttp
import time
from urllib.parse import quote_plus
from random import randint
from motor.motor_asyncio import AsyncIOMotorClient

async def negotiate(io_session: aiohttp.ClientSession):
    url = "https://cdn.ime.co.ir/realTimeServer/negotiate?clientProtocol=2.1&" \
          "connectionData=%5B%7B%22name%22%3A%22marketshub%22%7D%5D&_=" + str(int(time.time() * 1000))
    nego = await io_session.get(url)
    print(await nego.json())
    return await nego.json()


async def websocket(connection_token, io_session: aiohttp.ClientSession):
    url = "wss://cdn.ime.co.ir/realTimeServer/connect?transport=webSockets&clientProtocol=2.1&connectionToken=" + \
          quote_plus(connection_token) + "&connectionData=%5B%7B%22name%22%3A%22marketshub%22%7D%5D&tid=" + \
          str(randint(1, 10))
    dt = AsyncIOMotorClient('mongodb://localhost:27017')
    db = dt['imedb']
    collection = db['ime_collection']
    async with io_session.ws_connect(url) as ws:
        async for msg in ws:
          print(msg)
async def main():
    io_session = aiohttp.ClientSession()
    negot = await negotiate(io_session)
    await websocket(negot["ConnectionToken"], io_session)
    await io_session.close()

loop = asyncio.get_event_loop()
loop.create_task(main())
loop.run_forever()
