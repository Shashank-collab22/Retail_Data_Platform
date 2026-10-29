import logging

format="('%(asctime)s  %(levelname)s  %(message)s')"
def get_logger(name,level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format))
        logger.addHandler(handler)

    return logger
