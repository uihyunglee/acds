import abc
import datetime
import logging
import os
import threading
from copy import deepcopy

from queue import Queue

import pandas as pd
from sortedcontainers import SortedDict

from acds.core.publish import Publisher

logging = logging.getLogger('acds')


class OrderBookPublisher(Publisher):
    def __init__(self, ws_url, api_key, secret_key,
                 symbols, exchange, zmq_port,
                 save_mode=True):
        super().__init__(ws_url, api_key, secret_key,
                         symbols, exchange, zmq_port)

        self.symbols = symbols
        self.order_book = {symbol: OrderBook() for symbol in symbols}

        self.save_mode = save_mode
        if self.save_mode:
            self.data_buffer = {symbol: [] for symbol in symbols}
            self.tmp_data_buffer = {symbol: [] for symbol in symbols}
            self.data_snpshot = {}
            self.saving_flag = False
            self.save_q = Queue()

            self.save_dir = os.path.join(os.getcwd(), "data", "order_book")
            os.makedirs(self.save_dir, exist_ok=True)

            self.lock = threading.Lock()
            self.saving_thread = threading.Thread(
                target=self.periodic_save, daemon=True
            )
            self.saving_thread.start()

            self.save_time = self.get_next_save_time()

    @abc.abstractmethod
    def update_order_book(self, data, timeReceived):
        pass

    def publish_order_book(self, symbol, timeExchange,
                           timeReceived, timePublished):
        order_book = self.order_book[symbol]

        published_data = {
            'exchange': self.exchange,
            'symbol': symbol,
            'bidPrices': list(order_book.bids.keys()),
            'bidSizes': list(order_book.bids.values()),
            'askPrices': list(order_book.asks.keys()),
            'askSizes': list(order_book.asks.values()),
            'timeExchange': timeExchange,
            'timeReceived': timeReceived,
            'timePublished': timePublished,
        }
        message = {
            'topic': f'ORDERBOOK_{self.exchange}_{symbol}',
            'data': published_data
        }
        self.publisher_thread.publish(message)

        if self.save_mode:
            time_exchange = datetime.datetime.strptime(
                timeExchange[:26], '%Y-%m-%dT%H:%M:%S.%f'
            )
            if time_exchange > self.save_time:
                self.saving_flag = True

                save_time = self.save_time
                save_time = save_time.strftime('%Y%m%d%H%M')
                self.save_time = self.get_next_save_time()

                self.save_q.put(save_time)

            if self.saving_flag:
                self.tmp_data_buffer[symbol].append(published_data)
            else:
                self.data_buffer[symbol].append(published_data)

    def periodic_save(self):
        prev_save_time = datetime.datetime.now().strftime('%Y%m%d%H%M')
        while True:
            save_time = self.save_q.get()
            self.save_to_parquet(prev_save_time, save_time)
            prev_save_time = save_time

    def save_to_parquet(self, prev_save_time, save_time):
        self.data_snpshot = deepcopy(self.data_buffer)
        for symbol, data in self.data_snpshot.items():
            if not data:
                continue
            topic = f"orderbook_{self.exchange}_{symbol}"
            file_name = f'{topic}_{prev_save_time}_{save_time}.parquet'
            file_path = os.path.join(self.save_dir, file_name)

            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)
            logging.info(f"SAVE: {self.exchange}: {symbol}: {file_name}")

        with self.lock:
            self.data_buffer = self.tmp_data_buffer
            self.saving_flag = False
        self.tmp_data_buffer = {symbol: [] for symbol in self.symbols}

    @staticmethod
    def get_next_save_time():
        now = datetime.datetime.now(datetime.timezone.utc)
        save_hour = now.hour
        save_minute = (now.minute // 10 + 1) * 10
        shift_day = 0
        if save_minute == 60:
            save_hour += 1
            save_minute = 0
            if save_hour == 24:
                shift_day = 1
                save_hour = 0
        next_save_time = now.replace(
            hour=save_hour, minute=save_minute, second=0, microsecond=0
        ).replace(tzinfo=None)
        return next_save_time + datetime.timedelta(shift_day)


class OrderBook:
    def __init__(self):
        self.bids = SortedDict(lambda price: -price)
        self.asks = SortedDict()

    def update_order(self, price: float, quantity: float, side: str):
        book = self.bids if side == "bid" else self.asks
        if quantity == .0:
            if price in book:
                del book[price]
        else:
            book[price] = quantity
