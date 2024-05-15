import logging
from logging.handlers import RotatingFileHandler

# logging.basicConfig(filename = 'app.log',
#                     level = logging.DEBUG,
#                     format = '%(asctime)s:%(levelname)s:%(name)s:%(message)s')

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Also log to console.
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = RotatingFileHandler(
    'app.log', maxBytes=(1048576*5), backupCount=7
    )   
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    

    return logger