import abc
import logging
import os
import datetime
from copy import deepcopy
from threading import Thread
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
            self.save_q = Queue()
            
            self.save_dir = os.path.join(os.getcwd(), "data", "order_book")
            os.makedirs(self.save_dir, exist_ok=True)
            
            self.saving_thread = Thread(target=self.periodic_save, daemon=True)
            self.saving_thread.start()
        
            self.save_time = self.get_next_save_time()

    @abc.abstractmethod
    def update_order_book(self, data, timeReceived):
        pass

    def publish_order_book(self, symbol, timeExchange,
                           timeReceived, timePublished):
        order_book_instance = self.order_book[symbol]

        published_data = {
            'exchange': self.exchange,
            'symbol': symbol,
            'bidPrices': list(order_book_instance.bids.keys()),
            'bidSizes': list(order_book_instance.bids.values()),
            'askPrices': list(order_book_instance.asks.keys()),
            'askSizes': list(order_book_instance.asks.values()),
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
                data_buffer = deepcopy(self.data_buffer)
                self.data_buffer = {symbol: [] for symbol in self.symbols}
                
                self.save_q.put(data_buffer)
                self.save_time = self.get_next_save_time()
            
            self.data_buffer[symbol].append(published_data)

    def periodic_save(self):
        while True:
            data = self.save_q.get()
            if data:
                self.save_to_parquet(data)
    
    def save_to_parquet(self, data):
        for symbol, data in data.items():
            topic = f"orderbook_{self.exchange}_{symbol}"
            save_time = self.save_time.strftime('%Y%m%d%H%M')
            file_name = f'{topic}_{save_time}.parquet'
            file_path = os.path.join(self.save_dir, file_name)

            df = pd.DataFrame(data)
            df.to_parquet(file_path, index=False)

            logging.info(f"SAVE: {self.exchange}: {symbol}: {file_name}")
    
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
