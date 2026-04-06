import argparse
import asyncio
import logging
from datetime import datetime

from database_service import DatabaseService
from models import BidAsk, Pair
from ws_managers import KalshiWebsocketManager, PolymarketWebsocketManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] -- %(name)s/%(funcName)s::%(lineno)d -- %(message)s',
                    filename='./logs/app.log', filemode='a', datefmt="%m/%d/%y %H:%M:%S")

kalshi_logger = logging.getLogger("kalshi")
polymarket_logger = logging.getLogger("polymarket")
compare_logger = logging.getLogger("compare")
main_logger = logging.getLogger("main")


async def kalshi_websocket(db_service: DatabaseService, market_pair: Pair, queue: asyncio.Queue):
    kalshi_logger.debug(msg=f"Starting Kalshi websocket task with market ticker: {market_pair.first_exchange}")
    try:
        async with KalshiWebsocketManager(market_pair.first_exchange) as manager:
            while True:
                data = await manager.get_message()
                if data is None:
                    break

                msg_type = data.get("type")

                if msg_type == "ticker":
                    bid_ask = BidAsk(exchange="Kalshi", bid=float(data["msg"]["yes_bid_dollars"]),
                                     ask=float(data["msg"]["yes_ask_dollars"]), timestamp=datetime.now().timestamp())
                    await queue.put(bid_ask)
                    await db_service.save_bid_ask(market_pair.id, bid_ask)

                elif msg_type == "error":
                    kalshi_logger.error(data)

    except asyncio.CancelledError:
        kalshi_logger.warning("Task cancelled")
        raise
    except Exception as e:
        kalshi_logger.error(f"Unexpected error: {e}")
        raise


async def polymarket_websocket(db_service: DatabaseService, market_pair: Pair, queue: asyncio.Queue):
    polymarket_logger.debug(msg=f"Starting Polymarket websocket task with market ticker: {market_pair.second_id}")
    try:
        async with PolymarketWebsocketManager(market_pair.second_id) as manager:
            while True:
                data = await manager.get_message()
                if data is None:
                    break

                msg_type = data.get("event_type")

                if msg_type == "best_bid_ask":
                    bid_ask = BidAsk(exchange="Polymarket", bid=float(data["best_bid"]), ask=float(data["best_ask"]),
                                     timestamp=datetime.now().timestamp())
                    await queue.put(bid_ask)
                    await db_service.save_bid_ask(market_pair.id, bid_ask)

                elif msg_type == "error":
                    polymarket_logger.error(data)

    except asyncio.CancelledError:
        polymarket_logger.warning("Task cancelled")
        raise
    except Exception as e:
        polymarket_logger.error(f"Unexpected error: {e}")
        raise


async def compare_exchanges(db_service: DatabaseService, pair_id: int, queue, threshold=0.01):
    compare_logger.debug(f"Running with threshold: {threshold}")
    kalshi = None
    polymarket = None
    try:
        while True:
            bid_ask = await queue.get()
            if bid_ask.exchange == "Kalshi":
                kalshi = bid_ask
            else:
                polymarket = bid_ask

            if kalshi and polymarket:
                kalshi_long = polymarket.bid - kalshi.ask
                poly_long = kalshi.bid - polymarket.ask

                if kalshi_long > threshold or poly_long > threshold:
                    compare_logger.info(f"Arbitrage opportunity found")
                    if kalshi_long > threshold:
                        long_price = kalshi.ask
                        short_price = polymarket.bid
                        long_exchange = "Kalshi"
                    else:
                        long_price = polymarket.ask
                        short_price = kalshi.bid
                        long_exchange = "Polymarket"

                    db_service.save_signal(pair_id, long_price, short_price, long_exchange)

    except asyncio.CancelledError:
        compare_logger.warning("Task cancelled")
        raise


async def main(db_service, market_pair):
    queue = asyncio.Queue()
    tasks = [asyncio.create_task(kalshi_websocket(db_service, market_pair, queue)),
             asyncio.create_task(polymarket_websocket(db_service, market_pair, queue)),
             asyncio.create_task(compare_exchanges(db_service, market_pair.id, queue, threshold=0))]

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        for task in done:
            if task.exception():
                main_logger.error(f"Task error: {task.exception()}")

    except KeyboardInterrupt:
        main_logger.warning("Keyboard interrupt received, shutting down...")

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        main_logger.info("Cleaned up")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="simple arb")

    parser.add_argument("kalshi_ticker", type=str, help="Kalshi market ticker")
    parser.add_argument("poly_asset_id", type=str, help="Polymarket asset ID (found in clobTokenIds)")

    args = parser.parse_args()

    db_service = DatabaseService("prices.db")
    market_pair = db_service.get_or_create_pair("Kalshi", "Polymarket", args.kalshi_ticker, args.poly_asset_id)
    asyncio.run(main(db_service, market_pair))
