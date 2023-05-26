import logging
from ddcuimap.utils.setup_logging import log_setup


# CREATE LOGGER
log_setup()
logger = logging.getLogger("metamap_logger")
logger.propagate = False
logger.info("Initiating ddcuimap.metamap logging.")
