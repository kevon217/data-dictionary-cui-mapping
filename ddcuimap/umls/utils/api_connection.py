import os

import requests
from dotenv import load_dotenv

from ddcuimap.utils.decorators import log
from ddcuimap.umls import logger

load_dotenv()


@log(msg="Checking UMLS API credentials in config files or .env file")
def check_credentials(cfg):
    """Checks if api credentials exist in initialized config file or alternatively in an .env file"""

    if not cfg.apis.umls.user_info.apiKey:
        logger.warning("No API_KEY_UMLS found in config files. Looking in .env file.")
        try:
            apiKey = os.getenv("API_KEY_UMLS")
            logger.warning("Using API_KEY_UMLS found in .env file.")
            cfg.apis.umls.user_info.apiKey = apiKey
            cfg.apis.umls.query_params.apiKey = apiKey
        except ValueError:
            logger.error(
                "No API_KEY_UMLS in .env file. Please add your UMLS API key to configs.apis.config_umls_api.yaml OR .env file."
            )
            exit()
    if not cfg.apis.umls.user_info.email:
        logger.warning("No API_EMAIL_UMLS found in config files. Looking in .env file.")
        try:
            email = os.getenv("API_EMAIL_UMLS")
            logger.warning("Using API_EMAIL_UMLS found in .env file.")
            cfg.apis.umls.user_info.email = email
        except ValueError:
            logger.error(
                "No API_EMAIL_UMLS in .env file. Please add your UMLS API email to configs.apis.config_umls_api.yaml OR .env file."
            )
            exit()
    return cfg


@log(msg="Connecting to UMLS API")
def connect_to_umls(cfg):
    """Connects to UMLS API and prints response text"""

    payload = f"apikey={cfg.apis.umls.user_info.apiKey}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.request(
        "POST", cfg.apis.umls.api_settings.url, headers=headers, data=payload
    )
    logger.info(f"Response status: {response.status_code}")

    # TODO: write logic to handle response status codes, break if not 200

    return response
