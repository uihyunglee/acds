import datetime
import logging
import orjson

from acds.model.orderbook import OrderBookPublisher
from acds.utils.time import utc_now

logging = logging.getLogger('acds')


class BybitOrderBookPublisher(OrderBookPublisher):
    def subscribe(self, ws):
        message = {
            "op": "subscribe",
            "args": [f"orderbook.50.{symbol}" for symbol in self.symbols]
        }
        ws.send(orjson.dumps(message))
        logging.info(f"REQ: SUB: {self.exchange}: {message}")

    def websocket_handler(self, ws, message):
        if not isinstance(message, str):
            logging.warning(f"WARNING: SUB: {self.exchange}: {message}")
            return
        try:
            timeReceived = utc_now()
            data = orjson.loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error(f"ERROR: {self.exchange}: {e}")

    def update_order_book(self, data, timeReceived):
        if "data" not in data or not data["data"]:
            logging.warning(f"WARNING: SUB: {self.exchange}: {data}")
            return

        symbol = data.get("data", {}).get("s", "")
        if not symbol or symbol not in self.order_book:
            logging.warning(f"WARNING: SUB: {self.exchange}: symbol: {symbol}")
            return

        ts = data.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(
                float(ts) / 1000, datetime.timezone.utc
            ).isoformat(timespec='microseconds')
        else:
            timeExchange = "N/A"

        order_book = self.order_book[symbol]
        if data.get("type", "") == "snapshot":
            order_book.bids.clear()
            order_book.asks.clear()
            logging.debug(f"INIT: ORDERBOOK: {self.exchange}: {symbol}")

        bids = data["data"]["b"]
        asks = data["data"]["a"]

        for bid in bids:
            price = bid[0]
            quantity = bid[1]
            try:
                order_book.update_order(float(price), float(quantity), 'bid')
            except Exception as e:
                logging.error(f"ERROR: {self.exchange}: {symbol}: {e}")

        for ask in asks:
            price = ask[0]
            quantity = ask[1]
            try:
                order_book.update_order(float(price), float(quantity), 'ask')
            except Exception as e:
                logging.error(f"ERROR: {self.exchange}: {symbol}: {e}")

        timePublished = utc_now()
        self.publish_order_book(symbol, timeExchange, timeReceived, timePublished)
