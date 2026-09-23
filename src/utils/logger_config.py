"""
Pipeline Logging Infrastructure
Provides structured, readable, and secure logging with credential masking.
"""

import logging
import os
import re
import sys
from typing import Optional

# Pattern to identify and mask sensitive tokens in log outputs
SENSITIVE_PATTERNS = [
    re.compile(r'(password=)([^&\s]+)', re.IGNORECASE),
    re.compile(r'(secret=)([^&\s]+)', re.IGNORECASE),
    re.compile(r'(token=)([^&\s]+)', re.IGNORECASE),
    re.compile(r'(key=)([^&\s]+)', re.IGNORECASE),
]


class SensitiveDataFilter(logging.Filter):
    """Filter that masks passwords, API keys, and sensitive tokens."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            msg = record.msg
            for pattern in SENSITIVE_PATTERNS:
                msg = pattern.sub(r'\1******', msg)
            record.msg = msg
        return True


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Creates and configures a standardized logger with ANSI colors and timestamps.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        
        # Determine log level from param, env, or default to INFO
        log_level_name = level or os.getenv("LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_name, logging.INFO)
        logger.setLevel(log_level)
        handler.setLevel(log_level)

        # Standard data engineering log format
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        handler.addFilter(SensitiveDataFilter())

        logger.addHandler(handler)
        logger.propagate = False

    return logger
