"""
====================================================================================================
FILE: logging_config.py
PURPOSE: Logging Configuration Module - Setup and Management of Pipeline Logging 
====================================================================================================

DESCRIPTION:
    This module provides a standardized way to set up logging for the data processing pipeline.
    It configures loggers to output messages to the console with a consistent format that includes
    timestamps, logger names, run identifiers, log levels, and messages.

USAGE EXAMPLE:  
    from logging_config import setup_logging
    logger = setup_logging(name="voter_data", level=logging.INFO, run_id="20240615_001")
   
    logger.info("This is an informational message.")
    logger.error("This is an error message.")
    logger.debug("This is a debug message.")        
OUTPUT FORMAT:
    2024-06-15 12:00:00,000 | voter_data | 20240615_001 | INFO | This is an informational message.
    2024-06-15 12:00:00,001 | voter_data | 20240615_001 | ERROR | This is an error message.
    2024-06-15 12:00:00,002 | voter_data | 20240615_001 | DEBUG | This is a debug message.
    
VERSION HISTORY:
   - v1.0: Initial version created on 2026-01-20 by Lora Covrett   
===================================================================================================
"""

import logging
import sys

def setup_logging(name="voter_data", level=logging.INFO, run_id=None):
    """
    Set up logging configuration.

    Parameters:
    - name (str): The name of the logger.
    - level (int): The logging level (e.g., logging.INFO, logging.DEBUG).
    - run_id (str): An optional run identifier to include in log messages.
    """
    # Add run_id to log records if provided
    if run_id:
        class ContextFilter(logging.Filter):
            def filter(self, record):
                record.run_id = run_id
                return True

        logging.getLogger().addFilter(ContextFilter())
    else:
        logging.getLogger().addFilter(lambda record: setattr(record, 'run_id', 'N/A') or True)
    
    # Configure logger
    logger = logging.getLogger(name)

    # Set logging level
    logger.setLevel(level)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)

    # Set handler level
    handler.setLevel(level)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(run_id)s | %(levelname)s | %(message)s'
    )

    # Add formatter to handler
    handler.setFormatter(formatter)

    # Add handler to logger if not already added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger