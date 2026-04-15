import argparse
import asyncio
import logging
from datetime import datetime

from database_service import DatabaseService
from models import BidAsk, Pair, Exchange
from ws_managers import KalshiWebsocketManager, PolymarketWebsocketManager

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s.%(msecs)03d [%(levelname)s] -- %(name)s/%(funcName)s:%(lineno)d -- %(message)s',
                    filename='./logs/app.log', filemode='a', datefmt="%m/%d/%y %H:%M:%S")

kalshi_logger = logging.getLogger("kalshi")
polymarket_logger = logging.getLogger("polymarket")
compare_logger = logging.getLogger("compare")
main_logger = logging.getLogger("main")

BASE_RETRY_DELAY = 1  # second
MAX_RETRY_DELAY = 60  # second


async def kalshi_websocket(db_service: DatabaseService, market_pair: Pair, queue: asyncio.Queue):
    attempt = 0
    while True:
        try:
            async with KalshiWebsocketManager(market_pair.first_ticker) as manager:
                attempt = 0
                while True:
                    data = await manager.get_message()
                    if data is None:
                        break

                    msg_type = data.get("type")

                    if msg_type == "ticker":
                        kalshi_logger.debug(f"Received best_bid_ask: {data}")
                        bid_ask = BidAsk(exchange=Exchange.KALSHI, bid=float(data["msg"]["yes_bid_dollars"]),
                                         ask=float(data["msg"]["yes_ask_dollars"]),
                                         timestamp=datetime.now().timestamp())
                        await queue.put(bid_ask)
                        db_service.save_bid_ask(market_pair.id, bid_ask)

                    elif msg_type == "error":
                        kalshi_logger.error(data)
        except asyncio.CancelledError:
            kalshi_logger.warning("Task cancelled")
            raise
        except Exception as e:
            kalshi_logger.error(f"Unexpected error: {e}")

        delay = min(BASE_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
        kalshi_logger.info(f"Reconnecting in {delay} seconds...")
        await asyncio.sleep(delay)
        attempt += 1


async def polymarket_websocket(db_service: DatabaseService, market_pair: Pair, queue: asyncio.Queue):
    attempt = 0
    while True:
        try:
            async with PolymarketWebsocketManager(market_pair.second_ticker) as manager:
                attempt = 0
                while True:
                    data = await manager.get_message()
                    if data is None:
                        break

                    msg_type = data.get("event_type")

                    if msg_type == "best_bid_ask":
                        polymarket_logger.debug(f"Received best_bid_ask: {data}")
                        bid_ask = BidAsk(exchange=Exchange.POLYMARKET, bid=float(data["best_bid"]),
                                         ask=float(data["best_ask"]), timestamp=datetime.now().timestamp())
                        await queue.put(bid_ask)
                        db_service.save_bid_ask(market_pair.id, bid_ask)

                    elif msg_type == "error":
                        polymarket_logger.error(data)
        except asyncio.CancelledError:
            polymarket_logger.warning("Task cancelled")
            raise
        except Exception as e:
            polymarket_logger.error(f"Unexpected error: {e}")

        delay = min(BASE_RETRY_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
        polymarket_logger.info(f"Reconnecting in {delay} seconds...")
        await asyncio.sleep(delay)
        attempt += 1


async def compare_exchanges(db_service: DatabaseService, market_pair: Pair, queue, threshold=0.01, stale_limit=30):
    compare_logger.debug(f"Running with threshold: {threshold}, stale_limit: {stale_limit} seconds")
    first_bid_ask = None
    second_bid_ask = None
    try:
        while True:
            bid_ask: BidAsk = await queue.get()
            if bid_ask.exchange == Exchange.KALSHI:  # TODO: this is stupid code. Refactor to allow more exchanges.
                first_bid_ask = bid_ask
            else:
                second_bid_ask = bid_ask

            if first_bid_ask and second_bid_ask:
                if abs(first_bid_ask.timestamp - second_bid_ask.timestamp) > stale_limit:
                    compare_logger.warning(
                        f"Data may be stale, skipping comparison. first_bid_ask={first_bid_ask}, second_bid_ask={second_bid_ask}")
                    if first_bid_ask.timestamp < second_bid_ask.timestamp:
                        first_bid_ask = None
                    else:
                        second_bid_ask = None

                    continue

                first_long_spread = second_bid_ask.bid - first_bid_ask.ask
                second_long_spread = first_bid_ask.bid - second_bid_ask.ask

                if first_long_spread > threshold or second_long_spread > threshold:
                    compare_logger.info(f"Arbitrage opportunity found")
                    if first_long_spread > threshold:
                        long_price = first_bid_ask.ask
                        short_price = second_bid_ask.bid
                        long_exchange = Exchange.KALSHI
                    else:
                        long_price = second_bid_ask.ask
                        short_price = first_bid_ask.bid
                        long_exchange = Exchange.POLYMARKET

                    db_service.save_signal(market_pair.id, long_price, short_price, long_exchange)

    except asyncio.CancelledError:
        compare_logger.warning("Task cancelled")
        raise


async def flush_buffers_periodically(db_service: DatabaseService, interval: float = 5):
    main_logger.debug(f"Flush buffer setting (interval: {interval} seconds)")
    try:
        while True:
            await asyncio.sleep(interval)
            await db_service.flush_buffers()
    except asyncio.CancelledError:
        main_logger.warning("Flush task cancelled, flushing buffers before exit...")
        await db_service.flush_buffers()
        main_logger.info("Buffers flushed, exiting flush task")
        raise


async def main(db_service: DatabaseService, first_ticker: str, second_ticker: str, threshold: float, stale_limit: int,
               flush_interval: float):
    await db_service.initialize_db()
    market_pair = await db_service.get_or_create_pair(Exchange.KALSHI, Exchange.POLYMARKET, first_ticker, second_ticker)

    queue = asyncio.Queue()
    tasks = [
        asyncio.create_task(kalshi_websocket(db_service, market_pair, queue)),
        asyncio.create_task(polymarket_websocket(db_service, market_pair, queue)),
        asyncio.create_task(
            compare_exchanges(db_service, market_pair, queue, threshold=threshold, stale_limit=stale_limit)
        ),
        asyncio.create_task(flush_buffers_periodically(db_service, interval=flush_interval))
    ]

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

        for task in done:
            if not task.cancelled() and task.exception():
                main_logger.error(f"Task error: {task.exception()}")
    except KeyboardInterrupt:
        main_logger.warning("Keyboard interrupt received, shutting down...")

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)
        main_logger.info("Cleaned up")
    finally:
        await db_service.close_db()
        main_logger.info("Database connection closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="simple arb")

    parser.add_argument("kalshi_ticker", type=str, help="Kalshi market ticker")
    parser.add_argument("poly_asset_id", type=str, help="Polymarket asset ID (found in clobTokenIds)")
    parser.add_argument("--threshold", type=float, default=0.01, help="Arbitrage spread threshold (default: 0.01)")
    parser.add_argument("--stale_limit", type=int, default=30,
                        help="Maximum age of bid/ask data before validating for signal in seconds (default: 30)")
    parser.add_argument("--flush_interval", type=float, default=5, help="Buffer flush interval (default: 5)")

    args = parser.parse_args()

    db_service = DatabaseService("prices.db")
    asyncio.run(
        main(db_service, args.kalshi_ticker, args.poly_asset_id, args.threshold, args.stale_limit, args.flush_interval))
