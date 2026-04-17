import logging
from dataclasses import astuple
from datetime import datetime
from decimal import Decimal

import aiosqlite
from aiosqlite import Connection

from models import Pair, BidAsk

logger = logging.getLogger(__name__)


class DatabaseService:
    def __init__(self, db_path=None):
        self.db_path: str | None = db_path
        self.db_conn: Connection | None = None
        self._bid_ask_buffer: list[tuple[int, BidAsk]] = []
        self._signal_buffer: list[tuple] = []

    async def initialize_db(self):
        self.db_conn = await aiosqlite.connect(self.db_path)

        await self.db_conn.execute("PRAGMA journal_mode=WAL")
        await self.db_conn.execute("PRAGMA synchronous=NORMAL")
        await self.db_conn.execute("PRAGMA foreign_keys=ON")

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
                                        SELECT id, first_exchange, second_exchange, first_ticker, second_ticker
                                        FROM pair
                                        WHERE first_exchange = ?
                                          AND second_exchange = ?
                                          AND first_ticker = ?
                                          AND second_ticker = ?
                                        """, (first_exchange, second_exchange, first_ticker, second_ticker)) as cur:
            row = await cur.fetchone()

        return Pair(*row)

    def save_bid_ask(self, pair_id: int, bid_ask: BidAsk):
        logger.debug(f"Save to buffer bid and ask for pair {pair_id}, value: {bid_ask}")
        self._bid_ask_buffer.append((pair_id, bid_ask))

    def save_signal(self, pair_id: int, long_price: Decimal, short_price: Decimal, long_exchange: str):
        logger.debug(
            f"Saving to buffer signal for pair {pair_id}, long_price={long_price}, short_price={short_price}, long_exchange={long_exchange}")

        self._signal_buffer.append((pair_id, int(datetime.now().timestamp()), long_price, short_price, long_exchange))

    async def flush_buffers(self):
        if not self._bid_ask_buffer and not self._signal_buffer:
            return

        logger.debug(
            f"Flushing buffers: bid_ask_size: {len(self._bid_ask_buffer)}, signal_buffer_size: {len(self._signal_buffer)}")

        bid_ask_snapshot = self._bid_ask_buffer[:]
        signal_snapshot = self._signal_buffer[:]
        try:
            await self.db_conn.execute("BEGIN")
            if bid_ask_snapshot:
                await self.db_conn.executemany("""
                                               INSERT INTO bid_ask (pair_id, exchange, bid, ask, timestamp)
                                               VALUES (?, ?, ?, ?, ?)
                                               """, [(pair_id, *astuple(ba)) for pair_id, ba in bid_ask_snapshot])
            if signal_snapshot:
                await self.db_conn.executemany("""
                                               INSERT INTO signal (pair_id, timestamp, long_price, short_price, long_exchange)
                                               VALUES (?, ?, ?, ?, ?)
                                               """, signal_snapshot)
            await self.db_conn.commit()
        except Exception as e:
            logger.error(f"Failed to flush buffers: {e}")
            await self.db_conn.rollback()
            return

        del self._bid_ask_buffer[:len(bid_ask_snapshot)]
        del self._signal_buffer[:len(signal_snapshot)]

    async def close_db(self):
        if self.db_conn:
            await self.db_conn.close()
            logger.info(f"Closed database connection")
