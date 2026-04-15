import asyncio
import json
import logging

import websockets

from gen_auth_headers import gen_kalshi_auth_headers


class BaseWebsocketManager:
    url = None

    def __init__(self, logger_name=__name__):
        self.logger = logging.getLogger(logger_name)
        self.websocket = None
        self.is_subscribed = False
        self.is_connected = False

    def _get_additional_headers(self):
        return None

    async def connect(self):
        if self.url is None:
            raise NotImplementedError("Must implement url in subclass")

        try:
            self.websocket = await websockets.connect(self.url, additional_headers=self._get_additional_headers())

            self.is_connected = True
            self.logger.info(f"Connected")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            self.is_connected = False
            return False

    def _get_subscribe_message(self):
        raise NotImplementedError("Must implement subscribe in subclass")

    async def subscribe(self):
        if not self.is_connected:
            return False

        try:
            subscribe_message = self._get_subscribe_message()
            self.logger.debug(f"Sending subscribe message: {subscribe_message}")
            await self.websocket.send(json.dumps(subscribe_message))
            self.is_subscribed = True
            self.logger.info(f"Subscribed")
            return True
        except Exception as e:
            self.logger.error(f"{e}")
            self.is_subscribed = False
            return False

    def _get_unsubscribe_message(self):
        raise NotImplementedError("Must implement _get_unsubscribe_message in subclass")

    async def unsubscribe(self):
        if not self.is_subscribed:
            self.logger.warning("Already unsubscribed, skipping")
            return

        try:
            unsubscribe_message = self._get_unsubscribe_message()
            self.logger.debug(f"Sending unsubscribe message: {unsubscribe_message}")
            await self.websocket.send(json.dumps(unsubscribe_message))
            await asyncio.sleep(0.1)  # Wait for server to process
            self.is_subscribed = False
            self.logger.info(f"Unsubscribed")
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning(f"WebSocket already closed, cannot unsubscribe")
        except Exception as e:
            self.logger.error(f"Failed to unsubscribe: {e}")

    async def get_message(self):
        if not self.websocket:
            return None

        try:
            message = await self.websocket.recv()
            return json.loads(message)
        except websockets.exceptions.ConnectionClosed:
            self.is_connected = False
            self.is_subscribed = False
            self.logger.error(f"WebSocket already closed")
            return None
        except Exception as e:
            self.logger.error(f"Failed to get message: {e}")
            return None

    async def close(self):
        self.logger.debug(f"Closing WebSocket")
        if not self.websocket or not self.is_connected:
            self.logger.warning(f"WebSocket already closed")
            return

        await self.unsubscribe()

        try:
            await self.websocket.close()
            self.is_connected = False
            self.logger.info(f"WebSocket closed")
        except websockets.exceptions.ConnectionClosed:
            self.logger.warning(f"WebSocket already closed")
        except Exception as e:
            self.logger.error(f"{e}")

    async def __aenter__(self):
        if not await self.connect():
            raise ConnectionError("Failed to connect to WebSocket")
        await self.subscribe()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.logger.warning(f"Exiting (exc_type={exc_type}, exc_val='{exc_val}')")
        await self.close()


class KalshiWebsocketManager(BaseWebsocketManager):
    base_url = "wss://api.elections.kalshi.com"
    path = "/trade-api/ws/v2"
    url = base_url + path

    def __init__(self, market_ticker):
        super().__init__("KalshiWebsocket")
        self.logger.debug(f"Initializing KalshiWebsocketManager with market ticker: {market_ticker}")
        self.market_ticker = market_ticker

    def _get_additional_headers(self):
        return gen_kalshi_auth_headers("GET", self.path)

    def _get_subscribe_message(self):
        return {"id": 1, "cmd": "subscribe", "params": {"channels": ["ticker"], "market_ticker": self.market_ticker}}

    def _get_unsubscribe_message(self):
        return {"id": 1, "cmd": "unsubscribe", "params": {"channels": ["ticker"], "market_ticker": self.market_ticker}}


class PolymarketWebsocketManager(BaseWebsocketManager):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(self, assets_id):
        super().__init__("PolyWebsocket")
        self.logger.debug(f"Initializing PolymarketWebsocketManager with asset id: {assets_id}")
        self.assets_id = assets_id

    def _get_subscribe_message(self):
        return {"assets_ids": [self.assets_id], "type": "market", "custom_feature_enabled": True, "initial_dump": False}

    def _get_unsubscribe_message(self):
        return {"operation": "unsubscribe", "assets_ids": [self.assets_id], }
