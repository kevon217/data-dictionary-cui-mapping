import logging
from ddcuimap.utils.logger.config_logging import setup_log, log, copy_log

# CREATE LOGGER
setup_log()
umls_logger = logging.getLogger("umls_logger")
umls_logger.info("Initiating ddcuimap.umls logger.")
