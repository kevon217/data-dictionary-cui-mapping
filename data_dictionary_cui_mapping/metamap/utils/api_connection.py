import os

from dotenv import load_dotenv
from prefect import task

load_dotenv()


@task(name="Checking MetaMap UMLS API credentials in config files or .env file")
def check_credentials(cfg):
    """Checks if api credentials exist in initialized config file or alternatively in an .env file"""

    if not cfg.apis.user_info.apiKey:
        print("No API_KEY_UMLS found in config files. Looking in .env file.")
        try:
            apiKey = os.getenv("API_KEY_UMLS")
            print("Using API_KEY_UMLS found in .env file.")
            cfg.apis.user_info.apiKey = apiKey
        except ValueError:
            print(
                "No API_KEY_UMLS in .env file. Please add your UMLS API key to configs.apis.config_umls_api.yaml OR .env file."
            )
            exit()
    return cfg
