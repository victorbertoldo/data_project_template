import logging
import os
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file, level=logging.INFO):
    """Function to set up a logger with a rotating file handler and console output."""
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

    # File Handler
    file_handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=5)
    file_handler.setFormatter(formatter)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)  # Add console handler

    return logger
