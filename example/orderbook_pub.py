import time
from threading import Thread

from acds.exchange.okx import OkxOrderBookPublisher
# from acds.exchange.bybit import BybitOrderBookPublisher

from acds.config_example import EXCHANGE_CONFIG

TIME_SLEEP = 60 * 60 * 24 * 365

if __name__ == '__main__':
    pub_ob_okx = OkxOrderBookPublisher(
        ws_url=EXCHANGE_CONFIG["okx"]["ws_url"],
        symbols=["BTC-USDT", "ETH-USDT"],
        api_key='',
        secret_key='',
        exchange=EXCHANGE_CONFIG["okx"]["exchange"],
        zmq_port=EXCHANGE_CONFIG["okx"]["orderbook_port"],
        save_mode=True
    )
    # bybit_orderbook_publisher = BybitOrderBookPublisher(
    #     ws_url=EXCHANGE_CONFIG["bybit"]["ws_url"],
    #     symbols=["BTCUSDT", "ETHUSDT"],
    #     api_key='',
    #     secret_key='',
    #     exchange=EXCHANGE_CONFIG["bybit"]["exchange"],
    #     zmq_port=EXCHANGE_CONFIG["bybit"]["orderbook_port"]
    # )
    
    threads = []
    pubs = [v for k, v in globals().items() if k.startswith('pub')]
    for pub in pubs: threads.append(Thread(target=pub.start))

    for t in threads: t.start()

    time.sleep(TIME_SLEEP)

    for pub in pubs: pub.end()
    for t in threads: t.join()
