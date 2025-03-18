import logging
import os


def set_logger():
    logger = logging.getLogger('acds')
    logger.setLevel(logging.DEBUG)

    # FORMAT
    time = '%(asctime)s'
    level = '%(levelname)-8s'
    file = '%(filename)s'
    func = '%(funcName)s'
    line = '%(lineno)d'
    msg = '%(message)s'
    formatter = logging.Formatter(
        f'{time} | {level} {file} - {func} ({line}) :: {msg}'
    )

    # CONSOLE
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # FILE
    path = os.path.join(os.getcwd(), 'logs')
    os.makedirs(path, exist_ok=True)
    file_name = os.path.join(path, 'acds.log')
    file_handler = logging.FileHandler(filename=file_name)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # HANDLER
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
