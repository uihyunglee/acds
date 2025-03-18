import logging
import threading
import queue
import abc

import websocket
import zmq

logging = logging.getLogger('acds')


class Publisher(abc.ABC):
    def __init__(self, ws_url, api_key, secret_key, symbols, exchange, zmq_port):
        self.ws_url = ws_url
        self.api_key = api_key
        self.secret_key = secret_key
        self.symbols = symbols
        self.exchange = exchange
        self.zmq_port = zmq_port

        # INIT: SUB
        self.ws_app = None
        self.ws_thread = None
        
        # INIT: PUB
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher_thread = PubThread(self.publisher)

    @abc.abstractmethod
    def subscribe(self, ws):
        pass

    @abc.abstractmethod
    def websocket_handler(self, ws, message):
        pass

    def start(self):
        logging.info(f"START: {self.__class__.__name__}")

        # SUB
        self.ws_app = websocket.WebSocketApp(
            self.ws_url,
            on_open=lambda ws: self.subscribe(ws),
            on_message=self.websocket_handler
        )
        self.ws_thread = threading.Thread(target=self.ws_app.run_forever)
        self.ws_thread.daemon = True
        self.ws_thread.start()
        logging.info(f"SUB: {self.__class__.__name__}: {self.symbols}")

        # PUB
        self.publisher.bind(f"tcp://*:{self.zmq_port}")
        self.publisher_thread.start()
        logging.info(f"PUB: {self.__class__.__name__}: {self.zmq_port}")
        

    def end(self):
        logging.info(f"END: {self.__class__.__name__}")
        
        # SUB
        if self.ws_app is not None:
            self.ws_app.keep_running = False
            self.ws_app.close()
        if self.ws_thread is not None:
            self.ws_thread.join(timeout=2)
        
        # PUB
        self.publisher_thread.stop()
        self.publisher.close()
        self.context.term()


class PubThread(threading.Thread):
    def __init__(self, publisher):
        super().__init__()
        self.publisher = publisher
        self.queue = queue.Queue()
        self.daemon = True
        self.running = True

    def run(self):
        while self.running:
            msg = self.queue.get()
            self.publisher.send_json(msg)
            self.queue.task_done()

    def publish(self, msg):
        self.queue.put(msg)

    def stop(self):
        self.running = False
