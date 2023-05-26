import logging
from ddcuimap.utils.setup_logging import log_setup


# CREATE LOGGER
log_setup()
logger = logging.getLogger("helper_logger")
logger.propagate = False
