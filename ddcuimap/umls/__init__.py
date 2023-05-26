import logging
from ddcuimap.utils.setup_logging import log_setup


# CREATE LOGGER
log_setup()
logger = logging.getLogger("umls_logger")
logger.propagate = False
logger.info("Initiating ddcuimap.umls logging.")
