import logging
from ddcuimap.utils.logger.config_logging import setup_log, log, copy_log

# CREATE LOGGER
setup_log()
ss_logger = logging.getLogger("semantic_search_logger")
ss_logger.info("Initiating ddcuimap.semantic_search logger.")
