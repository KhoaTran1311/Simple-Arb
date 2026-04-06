import logging
from datetime import datetime

import aiosqlite

from models import Pair, BidAsk

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self, db_path=None):
        self.db_path = db_path
        self.db_conn = None

    async def initialize_db(self):
        self.db_conn = await aiosqlite.connect(self.db_path)

        query = """
                CREATE TABLE IF NOT EXISTS pair
                (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_exchange  TEXT,
                    second_exchange TEXT,
                    first_ticker    TEXT,
                    second_ticker   TEXT,
                    UNIQUE (first_exchange, second_exchange, first_ticker, second_ticker)
                );

                CREATE TABLE IF NOT EXISTS bid_ask
                (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id   INTEGER REFERENCES pair (id),
                    exchange  TEXT,
                    bid       REAL,
                    ask       REAL,
                    timestamp INTEGER
                );

                CREATE TABLE IF NOT EXISTS signal
                (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id       INTEGER REFERENCES pair (id),
                    timestamp     INTEGER,
                    long_price    REAL,
                    short_price   REAL,
                    long_exchange TEXT
                )
                """
        await self.db_conn.executescript(query)
        await self.db_conn.commit()
        logger.info(f"Initialized database")

    async def get_or_create_pair(self, first_exchange: str, second_exchange, first_ticker: str, second_ticker: str):
        logger.debug(f"Get/Create pair for [{first_exchange}]{first_ticker} –– [{second_exchange}]{second_ticker}")

        await self.db_conn.execute("""
                                   INSERT OR IGNORE INTO pair (first_exchange, second_exchange, first_ticker, second_ticker)
                                   VALUES (?, ?, ?, ?)
                                   """, (first_exchange, second_exchange, first_ticker, second_ticker))
        await self.db_conn.commit()
        async with self.db_conn.execute("""
                                        SELECT *
                                        FROM pair
                                        WHERE first_exchange = ?
                                          AND second_exchange = ?
                                          AND first_ticker = ?
                                          AND second_ticker = ?
                                        """, (first_exchange, second_exchange, first_ticker, second_ticker)) as cur:
            row = await cur.fetchone()

        return Pair(*row)

    async def save_bid_ask(self, pair_id: int, bid_ask: BidAsk):
        logger.debug(f"Save bid and ask for pair {pair_id}, value: {bid_ask}")

        await self.db_conn.execute("""
                                   INSERT INTO bid_ask (pair_id, exchange, bid, ask, timestamp)
                                   VALUES (?, ?, ?, ?, ?)
                                   """, (pair_id, bid_ask.exchange, bid_ask.bid, bid_ask.ask, int(bid_ask.timestamp)))
        await self.db_conn.commit()
        logger.info(f"Finished saving bid ask")

    async def save_signal(self, pair_id: int, long_price: float, short_price: float, long_exchange: str):
        logger.debug(
            f"Saving signal for pair {pair_id}, long_price={long_price}, short_price={short_price}, long_exchange={long_exchange}")

        await self.db_conn.execute("""
                                   INSERT INTO signal (pair_id, timestamp, long_price, short_price, long_exchange)
                                   VALUES (?, ?, ?, ?, ?)
                                   """,
                                   (pair_id, int(datetime.now().timestamp()), long_price, short_price, long_exchange))
        await self.db_conn.commit()
        logger.info(f"Finished saving signal")

    async def close_db(self):
        if self.db_conn:
            await self.db_conn.close()
            logger.info(f"Closed database connection")
