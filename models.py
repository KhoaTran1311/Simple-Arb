from dataclasses import dataclass
from enum import StrEnum


class Exchange(StrEnum):
    KALSHI = "Kalshi"
    POLYMARKET = "Polymarket"


@dataclass
class BidAsk:
    exchange: str
    bid: float
    ask: float
    timestamp: float

    def __str__(self):
        return f"BidAsk(exchange={self.exchange}, bid={self.bid}, ask={self.ask}, timestamp={self.timestamp})"


@dataclass
class Pair:
    id: int
    first_exchange: str
    second_exchange: str
    first_ticker: str
    second_ticker: str  # the ticker is the unique identifier for the market on the exchange, it is also known as asset_id on Polymarket and market_ticker on Kalshi.

    def __str__(self):
        return f"Pair([{self.first_exchange}] {self.first_id}, [{self.second_exchange}] {self.second_id})"
