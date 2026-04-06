import argparse
import asyncio
import logging
import sqlite3
from datetime import datetime

from ws_managers import KalshiWebsocketManager, PolymarketWebsocketManager

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s: [%(levelname)s] %(message)s',
                    filename='./logs/app.log', filemode='a')

kalshi_logger = logging.getLogger("kalshi")
polymarket_logger = logging.getLogger("polymarket")
compare_logger = logging.getLogger("compare")
main_logger = logging.getLogger("main")
db_logger = logging.getLogger("db")

# KALSHI_MARKET_TICKER = "KXNETFLIXRANKMOVIEGLOBAL-26APR06-PEA"  # "KXMARMAD-26-MICH"
# POLYMARKET_ASSET_ID = "11785106817903298677191578728525104664238359853143441532746222418350223391909"  # "104731530598978202925563656323694933154318837341770296468059903473029914991939"


class BidAsk:
    def __init__(self, exchange: str, bid: float, ask: float, timestamp: datetime):
        self.exchange = exchange
        self.bid = bid
        self.ask = ask
        self.timestamp = timestamp

    def __str__(self):
        return f"BidAsk(exchange={self.exchange}, bid={self.bid}, ask={self.ask}, timestamp={self.timestamp})"


async def kalshi_websocket(ticker, queue: asyncio.Queue):
    kalshi_logger.debug(msg=f"Starting Kalshi websocket task with market ticker: {ticker}")
    try:
        async with KalshiWebsocketManager(ticker) as manager:
            while True:
                data = await manager.get_message()
                if data is None:
                    break

                msg_type = data.get("type")

                if msg_type == "ticker":
                    bid_ask = BidAsk(exchange="Kalshi", bid=float(data["msg"]["yes_bid_dollars"]),
                                     ask=float(data["msg"]["yes_ask_dollars"]), timestamp=datetime.now())
                    await queue.put(bid_ask)
                    await save_bid_ask_to_db(bid_ask)

                elif msg_type == "error":
                    kalshi_logger.error(data)

    except asyncio.CancelledError:
        kalshi_logger.warning("Task cancelled")
        raise
    except Exception as e:
        kalshi_logger.error(f"Unexpected error: {e}")
        raise


async def polymarket_websocket(asset_id, queue: asyncio.Queue):
    polymarket_logger.debug(msg=f"Starting Polymarket websocket task with market ticker: {asset_id}")
    try:
        async with PolymarketWebsocketManager(asset_id) as manager:
            while True:
                data = await manager.get_message()
                if data is None:
                    break

                msg_type = data.get("event_type")

                if msg_type == "best_bid_ask":
                    bid_ask = BidAsk(exchange="Polymarket", bid=float(data["best_bid"]), ask=float(data["best_ask"]),
                                     timestamp=datetime.now())
                    await queue.put(bid_ask)
                    await save_bid_ask_to_db(bid_ask)

                elif msg_type == "error":
                    polymarket_logger.error(data)

    except asyncio.CancelledError:
        polymarket_logger.warning("Task cancelled")
        raise
    except Exception as e:
        polymarket_logger.error(f"Unexpected error: {e}")
        raise


async def make_trade(kalshi: BidAsk, polymarket: BidAsk, is_first_long: bool):
    compare_logger.info(f"Executing trade: {kalshi}, {polymarket}, is_first_long={is_first_long}")
    if is_first_long:
        long_price = kalshi.ask
        short_price = polymarket.bid
        long_exchange = "Kalshi"
    else:
        long_price = polymarket.ask
        short_price = kalshi.bid
        long_exchange = "Polymarket"

    await save_trade_to_db(long_price, short_price, long_exchange)


async def compare_exchanges(queue, threshold=0.01):
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

                if kalshi_long > threshold:
                    await make_trade(kalshi, polymarket, is_first_long=True)
                if poly_long > threshold:
                    await make_trade(kalshi, polymarket, is_first_long=False)

    except asyncio.CancelledError:
        compare_logger.warning("Task cancelled")
        raise


async def main(kalshi_ticker: str, poly_asset_id: str):
    queue = asyncio.Queue()
    tasks = [asyncio.create_task(kalshi_websocket(kalshi_ticker, queue)),
             asyncio.create_task(polymarket_websocket(poly_asset_id, queue)),
             asyncio.create_task(compare_exchanges(queue, threshold=0))]

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


def setup_db():
    db_logger.debug(f"Setting up database")
    con = sqlite3.connect("prices.db")
    cur = con.cursor()
    cur.execute("""
                CREATE TABLE IF NOT EXISTS bid_ask
                (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    exchange  TEXT,
                    bid       REAL,
                    ask       REAL,
                    timestamp INTEGER
                )
                """)

    cur.execute("""
                CREATE TABLE IF NOT EXISTS trades
                (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp     INTEGER,
                    long_price    REAL,
                    short_price   REAL,
                    long_exchange TEXT
                )
                """)
    con.commit()
    con.close()
    db_logger.debug(f"Finished setting up database")


async def save_trade_to_db(long_price: float, short_price: float, long_exchange: str):
    db_logger.debug(f"Saving trade: long_price={long_price}, short_price={short_price}, long_exchange={long_exchange}")
    con = sqlite3.connect("prices.db")
    cur = con.cursor()
    cur.execute("""
                INSERT INTO trades (timestamp, long_price, short_price, long_exchange)
                VALUES (?, ?, ?, ?)
                """, (int(datetime.now().timestamp()), long_price, short_price, long_exchange))
    con.commit()
    con.close()
    db_logger.debug(f"Finished saving trade")


async def save_bid_ask_to_db(bid_ask: BidAsk):
    db_logger.debug(f"Saving {bid_ask}")
    con = sqlite3.connect("prices.db")
    cur = con.cursor()
    cur.execute("""
                INSERT INTO bid_ask (exchange, bid, ask, timestamp)
                VALUES (?, ?, ?, ?)
                """, (bid_ask.exchange, bid_ask.bid, bid_ask.ask, int(bid_ask.timestamp.timestamp())))
    con.commit()
    con.close()
    db_logger.debug(f"Finished saving {bid_ask}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="simple arb")

    parser.add_argument("kalshi_ticker", type=str, help="Kalshi market ticker")
    parser.add_argument("poly_asset_id", type=str, help="Polymarket asset ID (found in clobTokenIds)")

    args = parser.parse_args()

    setup_db()
    asyncio.run(main(args.kalshi_ticker, args.poly_asset_id))
