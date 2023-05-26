import logging
import logging.config
import coloredlogs
import os
import yaml

DEFAULT_LEVEL = logging.INFO


def log_setup(fp_cfg_logging="../configs/logging/logging.yaml"):
    if os.path.exists(fp_cfg_logging):
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
                print("Error with file, using Default logging")
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
        print("Config file not found, using Default logging")


if __name__ == "__main__":
    log_setup()
    logger = logging.getLogger(__name__)
    logger.info("Setting up logging.")
