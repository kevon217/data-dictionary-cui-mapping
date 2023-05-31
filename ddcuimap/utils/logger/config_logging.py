"""

Logging configuration for the ddcuimap package.

"""


import functools
import logging
import logging.config
import coloredlogs
import os
import yaml
from pathlib import Path
import shutil


# DEFAULTS
DEFAULT_LEVEL = logging.INFO

# LOG DECORATOR


def log(msg=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Retrieve the logger dynamically
            logger = logging.getLogger(func.__module__)
            # logger = logger.getLogger(func.__module__.split('.')[0])
            # Log the custom message if provided
            if msg is not None:
                logger.info(msg)

            # Call the original function
            result = func(*args, **kwargs)

            return result

        return wrapper

    return decorator


# MODULE LOGGING CONFIGURATION
fp_cfg_logging = Path(__file__).parent / "config_logging.yaml"


def setup_log(fp_cfg_logging=fp_cfg_logging):
    if fp_cfg_logging.is_file():
        with open(fp_cfg_logging, "rt") as cfg_logging:
            try:
                cfg_log = yaml.safe_load(cfg_logging.read())
                logging.config.dictConfig(cfg_log)
                coloredlogs.install(
                    fmt=cfg_log["formatters"]["coloredlogs"]["format"],
                    level_styles=cfg_log["formatters"]["coloredlogs"]["level_styles"],
                    field_styles=cfg_log["formatters"]["coloredlogs"]["field_styles"],
                )
            except Exception as e:
                print("Error with file, using Default logger")
                logging.basicConfig(level=DEFAULT_LEVEL)
                coloredlogs.install(
                    level=DEFAULT_LEVEL,
                    fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level_styles=cfg_log["formatters"]["coloredlogs"]["level_styles"],
                    field_styles=cfg_log["formatters"]["coloredlogs"]["field_styles"],
                )
    else:
        logging.basicConfig(level=DEFAULT_LEVEL)
        coloredlogs.install(
            level=DEFAULT_LEVEL,
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            level_styles=dict(
                debug=dict(color="white"),
                info=dict(color="blue"),
                warning=dict(color="yellow", bright=True),
                error=dict(color="red", bold=True, bright=True),
                critical=dict(color="black", bold=True, background="red"),
            ),
            field_styles=dict(
                name=dict(color="white"),
                asctime=dict(color="white"),
                funcName=dict(color="white"),
                lineno=dict(color="white"),
            ),
        )
        print("Config file not found, using Default logger")


@log("Moving log file to new directory")
def move_log(logger, new_dir):
    """Moves log file to new directory"""

    fp_log_orig = Path(logger.handlers[0].baseFilename)
    fp_log_new = Path(new_dir / Path(logger.handlers[0].baseFilename).name)
    logger.handlers[0].close()
    shutil.move(fp_log_orig, fp_log_new)


@log("Copying log file to new directory")
def copy_log(logger, new_dir, new_filename=None):
    """Moves log file to new directory"""

    fp_log_orig = Path(logger.handlers[0].baseFilename)
    if new_filename:
        fp_log_new = Path(new_dir) / Path(new_filename)
    else:
        fp_log_new = Path(new_dir) / Path(logger.handlers[0].baseFilename).name
    logger.handlers[0].close()
    shutil.copy(fp_log_orig, fp_log_new)


# def log_to_dir(logger, dir_log, name_log):
#     """Adds a FileHandler for a user-defined directory to output script run log."""
#     fp_log = Path(dir_log / f"{name_log}")
#     logger.handlers[0].baseFilename = fp_log


if __name__ == "__main__":
    setup_log()
    logger = logging.getLogger(__name__)
    logger.info("Setting up logger.")
