import logging
from ddcuimap.utils.logger.config_logging import setup_log, log, copy_log

# CREATE LOGGER
setup_log()
mm_logger = logging.getLogger("metamap_logger")
mm_logger.info("Initiating ddcuimap.metamap logger.")
