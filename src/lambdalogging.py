"""
Lambda logging helper.

Returns a Logger with log level set based on env variables.
"""

import logging

from config import LOG_LEVEL

# translate log level from string to numeric value
LOG_LEVEL = getattr(logging, LOG_LEVEL) if hasattr(logging, LOG_LEVEL) else logging.DEBUG


def getLogger(name):
    """Return a logger configured based on env variables."""
    logger = logging.getLogger(name)
    # in lambda environment, logging config has already been setup so can't use logging.basicConfig to change log level
    logger.setLevel(LOG_LEVEL)
    return logger
