import os

from dotenv import load_dotenv

load_dotenv()

import pinecone

from ddcuimap.utils.decorators import log
from ddcuimap.semantic_search import logger


@log(msg="Checking Pinecone credentials in config files or .env file")
def check_credentials(cfg):
    """Checks if api credentials exist in initialized config file or alternatively in an .env file"""

    if not cfg.apis.pinecone.index_info.apiKey:
        logger.warning("No apiKey found in config files. Looking in .env file.")
        try:
            apiKey = os.getenv("API_KEY_PINECONE")
            logger.info("Using API_KEY_PINECONE found in .env file.")
            cfg.apis.pinecone.index_info.apiKey = apiKey
        except ValueError:
            logger.error(
                "No API_KEY_PINECONE in .env file. Please add your Pinecone API key to configs.apis.config_pinecone_api.yaml OR .env file."
            )
            exit()
    if not cfg.apis.pinecone.index_info.environment:
        logger.warning("No environment found in config files. Looking in .env file.")
        try:
            environment = os.getenv("API_ENV_PINECONE")
            logger.info("Using API_KEY_PINECONE found in .env file.")
            cfg.apis.pinecone.index_info.environment = environment
        except ValueError:
            logger.error(
                "No API_ENV_PINECONE in .env file. Please add your Pinecone API environment to configs.apis.config_pinecone_api.yaml OR .env file."
            )
            exit()

    return cfg


@log(msg="Connecting to Pinecone index")
def connect_to_pinecone(cfg):
    """Connects to Pinecone API and print index info"""

    pinecone.init(
        api_key=cfg.apis.pinecone.index_info.apiKey,
        environment=cfg.apis.pinecone.index_info.environment,
    )

    return pinecone
