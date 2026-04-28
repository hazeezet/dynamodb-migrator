import logging
import sys

# Configure logging
logging.basicConfig(
    filename='migration.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def get_logger():
    """Get the configured logger instance."""
    return logging.getLogger(__name__)
