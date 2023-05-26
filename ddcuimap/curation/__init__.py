import logging
from ddcuimap.utils.setup_logging import log_setup


# CREATE LOGGER
log_setup()
logger = logging.getLogger("curation_logger")
logger.propagate = False
