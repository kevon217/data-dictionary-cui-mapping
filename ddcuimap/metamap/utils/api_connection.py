import os

from dotenv import load_dotenv

load_dotenv()

from ddcuimap.metamap import logger
from ddcuimap.utils.decorators import log


@log(msg="Checking MetaMap UMLS API credentials in config files or .env file")
def check_credentials(cfg):
    """Checks if api credentials exist in initialized config file or alternatively in an .env file"""

    if not cfg.apis.metamap.user_info.apiKey:
        logger.warning("No API_KEY_UMLS found in config files. Looking in .env file.")
        try:
            apiKey = os.getenv("API_KEY_UMLS")
            logger.info("Using API_KEY_UMLS found in .env file.")
            cfg.apis.metamap.user_info.apiKey = apiKey
        except ValueError:
            logger.error(
                "No API_KEY_UMLS in .env file. Please add your UMLS API key to configs.apis.config_umls_api.yaml OR .env file."
            )
            exit()
    if not cfg.apis.metamap.user_info.email:
        logger.warning("No API_EMAIL_UMLS found in config files. Looking in .env file.")
        try:
            email = os.getenv("API_EMAIL_UMLS")
            logger.info("Using API_EMAIL_UMLS found in .env file.")
            cfg.apis.metamap.user_info.email = email
        except ValueError:
            logger.error(
                "No API_EMAIL_UMLS in .env file. Please add your UMLS API email to configs.apis.config_metamap_api.yaml OR .env file."
            )
            exit()
    return cfg
