import datetime
import logging

import orjson

from acds.model.orderbook import OrderBookPublisher

logging = logging.getLogger('acds')


class OkxOrderBookPublisher(OrderBookPublisher):
    def subscribe(self, ws):
        message = {
            "op": "subscribe",
            "args": [
                {"channel": "books5", "instId": symbol}
                for symbol in self.symbols
            ]
        }
        ws.send(orjson.dumps(message))
        logging.info(f"REQ: SUB: {self.exchange}: {message}")

    def websocket_handler(self, ws, message):
        if not isinstance(message, str):
            logging.warning(f"WARNING: SUB: {self.exchange}: {message}")
            return
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            timeReceived = now_utc.isoformat(timespec='microseconds')
            data = orjson.loads(message)
            self.update_order_book(data, timeReceived)
        except Exception as e:
            logging.error(f"ERROR: {self.exchange}: {e}")

    def update_order_book(self, data, timeReceived):
        if "data" not in data or not data["data"]:
            logging.warning(f"WARNING: SUB: {self.exchange}: {data}")
            return
        
        symbol = data.get("arg", {}).get("instId")
        if not symbol or symbol not in self.order_book:
            logging.warning(f"WARNING: SUB: {self.exchange}: symbol: {symbol}")
            return
        
        data_item = data["data"][0]
        ts = data_item.get("ts")
        if ts:
            timeExchange = datetime.datetime.fromtimestamp(
                float(ts) / 1000, datetime.timezone.utc
            ).isoformat(timespec='microseconds')
        else:
            timeExchange = "N/A"

        ob = self.order_book[symbol]

        if "bids" in data_item:
            for bid in data_item["bids"]:
                try:
                    price, quantity = bid[:2]
                    ob.update_order(float(price), float(quantity), "bid")
                except Exception as e:
                    logging.error("%s: Error updating bid at price %s for %s: %s", self.exchange, price, symbol, e)

        if "asks" in data_item:
            for ask in data_item["asks"]:
                try:
                    price, quantity = ask[:2]
                    ob.update_order(float(price), float(quantity), "ask")
                except Exception as e:
                    logging.error("%s: Error updating ask at price %s for %s: %s", self.exchange, price, symbol, e)
                timePublished = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec='microseconds')

        self.publish_order_book(symbol, timeExchange, timeReceived, timePublished)
