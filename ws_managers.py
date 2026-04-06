import asyncio
import json
import logging

import websockets
from websockets.client import ClientConnection

from gen_auth_headers import gen_kalshi_auth_headers

kalshi_logger = logging.getLogger("kalshi")
polymarket_logger = logging.getLogger("polymarket")


class KalshiWebsocketManager:
    KALSHI_WS_BASE_URL = "wss://api.elections.kalshi.com"
    KALSHI_WS_PATH = "/trade-api/ws/v2"

    def __init__(self, market_ticker):
        self.websocket = None
        self.is_connected = False
        self.is_subscribed = False
        self.market_ticker = market_ticker

    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.KALSHI_WS_BASE_URL + self.KALSHI_WS_PATH,
                additional_headers=gen_kalshi_auth_headers("GET", self.KALSHI_WS_PATH))

            self.is_connected = True
            kalshi_logger.info(msg=f"Connected")
            return True
        except Exception as e:
            kalshi_logger.error(f"Failed to connect: {e}")
            self.is_connected = False
            return False

    async def subscribe(self):
        if not self.is_connected:
            return False

        try:
            subscribe_msg = {"id": 1, "cmd": "subscribe",
                             "params": {"channels": ["ticker"], "market_ticker": self.market_ticker}}
            await self.websocket.send(json.dumps(subscribe_msg))
            self.is_subscribed = True
            kalshi_logger.info(msg=f"Subscribed")
            return True
        except Exception as e:
            kalshi_logger.error(f"Failed to subscribe: {e}")
            self.is_subscribed = False
            return False

    async def unsubscribe(self):
        kalshi_logger.debug(msg=f"Unsubscribing")
        if not self.is_subscribed:
            return

        try:
            unsubscribe_msg = {"id": 1, "cmd": "unsubscribe",
                               "params": {"channels": ["ticker"], "market_ticker": self.market_ticker}}
            kalshi_logger.debug(msg=f"Sending unsubscribe message")
            await self.websocket.send(json.dumps(unsubscribe_msg))
            await asyncio.sleep(0.1)  # Wait for server to process
            self.is_subscribed = False
            kalshi_logger.info(msg=f"Unsubscribed")
        except websockets.exceptions.ConnectionClosed:
            polymarket_logger.warning(f"WebSocket already closed, cannot unsubscribe")
        except Exception as e:
            kalshi_logger.error(f"Failed to unsubscribe: {e}")

    async def get_message(self):
        if not self.websocket:
            return None

        try:
            message = await self.websocket.recv()
            return json.loads(message)
        except asyncio.TimeoutError:
            kalshi_logger.warning(msg=f"Timeout while waiting for message")
            return None
        except websockets.exceptions.ConnectionClosed:
            kalshi_logger.error(f"WebSocket already closed")
        except Exception as e:
            kalshi_logger.error(f"Failed to get message: {e}")
            return None

    async def close(self):
        kalshi_logger.debug(msg=f"Closing WebSocket")
        if not self.websocket:
            kalshi_logger.warning(f"WebSocket already closed")
            return

        await self.unsubscribe()

        try:
            await self.websocket.close()
            self.is_connected = False
            kalshi_logger.info(msg=f"WebSocket closed")
        except websockets.exceptions.ConnectionClosed:
            kalshi_logger.warning(f"WebSocket already closed")
        except Exception as e:
            kalshi_logger.error(f"Failed to close WebSocket: {e}")

    async def __aenter__(self):
        await self.connect()
        await self.subscribe()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        kalshi_logger.warning(msg=f"Exiting (exc_type={exc_type})")
        await self.close()


class PolymarketWebsocketManager:
    POLYMARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, assets_id):
        self.websocket: None | ClientConnection = None
        self.is_connected = False
        self.is_subscribed = False
        self.assets_id = assets_id

    async def connect(self):
        try:
            self.websocket = await websockets.connect(self.POLYMARKET_WS_URL)
            self.is_connected = True
            polymarket_logger.info(msg=f"Connected")
            return True
        except Exception as e:
            polymarket_logger.error(f"Failed to connect: {e}")
            self.is_connected = False
            return False

    async def subscribe(self):
        if not self.is_connected:
            return False

        try:
            subscribe_msg = {"assets_ids": [self.assets_id], "type": "market", "custom_feature_enabled": True,
                             "initial_dump": False}
            await self.websocket.send(json.dumps(subscribe_msg))
            self.is_subscribed = True
            polymarket_logger.info(msg=f"Subscribed")
            return True
        except Exception as e:
            polymarket_logger.error(f"Failed to subscribe: {e}")
            self.is_subscribed = False
            return False

    async def unsubscribe(self):
        polymarket_logger.debug(msg=f"Unsubscribing")
        if not self.is_subscribed:
            return

        try:
            unsubscribe_msg = {"operation": "unsubscribe", "assets_ids": [self.assets_id], }
            polymarket_logger.debug(msg=f"Sending unsubscribe message")
            await self.websocket.send(json.dumps(unsubscribe_msg))
            await asyncio.sleep(0.1)  # Wait for server to process
            self.is_subscribed = False
            polymarket_logger.info(msg=f"Sending unsubscribe message")
        except websockets.exceptions.ConnectionClosed:
            polymarket_logger.warning(f"WebSocket already closed, cannot unsubscribe")
            self.is_subscribed = False
        except Exception as e:
            polymarket_logger.error(f"Failed to unsubscribe: {e}")

    async def get_message(self):
        if not self.websocket:
            return None

        try:
            message = await self.websocket.recv()
            return json.loads(message)
        except asyncio.TimeoutError:
            polymarket_logger.warning(msg=f"Timeout while waiting for message")
            return None
        except websockets.exceptions.ConnectionClosed:
            polymarket_logger.error(f"WebSocket already closed")
        except Exception as e:
            polymarket_logger.error(f"Failed to get message: {e}")
            return None

    async def close(self):
        polymarket_logger.debug(msg=f"Closing WebSocket")
        if not self.websocket:
            polymarket_logger.warning(f"WebSocket already closed")
            return

        await self.unsubscribe()

        try:
            await self.websocket.close()
            self.is_connected = False
            polymarket_logger.info(msg=f"WebSocket closed")
        except websockets.exceptions.ConnectionClosed:
            polymarket_logger.warning(f"WebSocket already closed")
        except Exception as e:
            polymarket_logger.error(f"Failed to close WebSocket: {e}")

    async def __aenter__(self):
        await self.connect()
        await self.subscribe()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        polymarket_logger.warning(msg=f"Exiting (exc_type={exc_type})")
        await self.close()
