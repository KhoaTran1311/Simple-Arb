from decimal import Decimal


class Orderbook:
    def __init__(self, asset_id=None):
        self.bid: dict[Decimal, Decimal] = {}
        self.ask: dict[Decimal, Decimal] = {}
        self.poly_asset_id = asset_id

    def populate_polymarket(self, data: dict):
        self.bid = {Decimal(pl['price']): Decimal(pl['size']) for pl in data['bids']}
        self.ask = {Decimal(pl['price']): Decimal(pl['size']) for pl in data['asks']}

    def delta_polymarket(self, data):
        """
        :param data: taken from price_changes field from polymarket's response
        :return:
        """
        entry = next((d for d in data if d['asset_id'] == self.poly_asset_id), None)
        if entry is None:
            raise ValueError(f"asset_id {self.poly_asset_id} not found in price_change data: {data}")
        size = Decimal(entry['size'])
        price = Decimal(entry['price'])
        if entry['side'] == 'BUY':
            if size == Decimal(0):  # TODO: order if else
                self.bid.pop(price, None)
            else:
                self.bid[price] = size
        else:
            if size == Decimal(0):
                self.ask.pop(price, None)
            else:
                self.ask[price] = size

    def populate_kalshi(self, data_msg: dict):
        """
        :param data_msg: taken from msg field of the response
        :return:
        """
        self.bid = {Decimal(pl[0]): Decimal(pl[1]) for pl in data_msg.get('yes_dollars_fp', [])}
        self.ask = {1 - Decimal(pl[0]): Decimal(pl[1]) for pl in
                    data_msg.get('no_dollars_fp', [])}  # reversed order take this into consideration when upgrade

    def delta_kalshi(self, data_msg: dict):
        price = Decimal(data_msg['price_dollars'])
        if data_msg.get('side') == 'yes':
            self.bid[price] = self.bid.get(price, Decimal(0)) + Decimal(data_msg['delta_fp'])
            if self.bid[price] <= Decimal(0):
                self.bid.pop(price, None)
        else:
            price = 1 - price
            self.ask[price] = self.ask.get(price, Decimal(0)) + Decimal(data_msg['delta_fp'])
            if self.ask[price] <= Decimal(0):
                self.ask.pop(price, None)

    def best_bid(self) -> Decimal | None:
        return max(self.bid) if self.bid else None

    def best_ask(self) -> Decimal | None:
        return min(self.ask) if self.ask else None
