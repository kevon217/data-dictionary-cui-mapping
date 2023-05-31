import logging
from ddcuimap.utils.logger.config_logging import setup_log, log, copy_log

# CREATE LOGGER
setup_log()
hydra_logger = logging.getLogger("hydra_search_logger")
hydra_logger.info("Initiating ddcuimap.hydra_search logger.")
