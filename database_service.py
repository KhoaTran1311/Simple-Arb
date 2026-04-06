import logging
import sqlite3
from datetime import datetime

from models import Pair, BidAsk

db_logger = logging.getLogger("db")


class DatabaseService:
    def __init__(self, db_path=None):
        self.db_path = db_path
        self.connection = None
        self._initialize_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _initialize_db(self):
        query = """
                CREATE TABLE IF NOT EXISTS pair
                (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    first_exchange  TEXT,
                    second_exchange TEXT,
                    first_id        TEXT,
                    second_id       TEXT,
                    UNIQUE (first_exchange, second_exchange, first_id, second_id)
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
        with self._get_connection() as conn:
            db_logger.debug(f"Initializing up database")
            conn.executescript(query)

    def get_or_create_pair(self, first_exchange: str, second_exchange, first_id: str, second_id: str):
        db_logger.debug(f"Get/Create pair for [{first_exchange}]{first_id} –– [{second_exchange}]{second_id}")

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                        SELECT *
                        FROM pair
                        WHERE first_exchange = ?
                          AND second_exchange = ?
                          AND first_id = ?
                          AND second_id = ?
                        """, (first_exchange, second_exchange, first_id, second_id))
            row = cur.fetchone()
            if row:
                db_logger.debug(f"Found existing pair with id: {row[0]}")
                market_pair = Pair(*row)
                db_logger.debug(f"Found pair: {market_pair}")
                return market_pair

            cur.execute("""
                        INSERT INTO pair (first_exchange, second_exchange, first_id, second_id)
                        VALUES (?, ?, ?, ?)
                        """, (first_exchange, second_exchange, first_id, second_id))

            pair_id = cur.lastrowid
            db_logger.debug(f"Created new pair id: {pair_id}")
            return Pair(pair_id, first_exchange, second_exchange, first_id, second_id)


    async def save_bid_ask(self, pair_id: int, bid_ask: BidAsk):
        db_logger.debug(f"Save bid and ask for pair {pair_id}, value: {bid_ask}")
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                        INSERT INTO bid_ask (exchange, bid, ask, timestamp, pair_id)
                        VALUES (?, ?, ?, ?, ?)
                        """, (bid_ask.exchange, bid_ask.bid, bid_ask.ask, int(bid_ask.timestamp), pair_id))
            conn.close()
            db_logger.debug(f"Finished saving {bid_ask}")


    def save_signal(self, pair_id: int, long_price: float, short_price: float, long_exchange: str):
        db_logger.debug(f"Saving signal for pair {pair_id}, long_price={long_price}, short_price={short_price}, long_exchange={long_exchange}")
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                        INSERT INTO signal (pair_id, timestamp, long_price, short_price, long_exchange)
                        VALUES (?, ?, ?, ?, ?)
                        """, (pair_id, int(datetime.now().timestamp()), long_price, short_price, long_exchange))
            conn.close()
            db_logger.debug(f"Finished saving signal")


