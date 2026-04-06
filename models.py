from dataclasses import dataclass


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
    first_id: str
    second_id: str

    def __str__(self):
        return f"Pair([{self.first_exchange}] {self.first_id}, [{self.second_exchange}] {self.second_id})"
