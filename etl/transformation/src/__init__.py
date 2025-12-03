import logging
import os
import sys


# Configure logging for the entire package
def configure_logging(level=logging.INFO):
    """Configure logging with a consistent format across the package."""

    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add our handler
    root_logger.addHandler(console_handler)

    # Configure package loggers
    package_logger = logging.getLogger(__name__.split(".")[0])
    package_logger.setLevel(level)

    return root_logger


# Configure logging when the package is imported
if os.getenv("DEBUG", "false").lower() == "true":
    configure_logging(level=logging.DEBUG)
else:
    configure_logging(level=logging.INFO)

# Export the configuration function for manual use
__all__ = ["configure_logging"]
