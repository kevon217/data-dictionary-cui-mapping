import os

import requests
from dotenv import load_dotenv
from prefect import task

load_dotenv()


@task(name="Checking UMLS API credentials in config files or .env file")
def check_credentials(cfg):
    """Checks if api credentials exist in initialized config file or alternatively in an .env file"""

    if not cfg.apis.user_info.apiKey:
        print("No API_KEY_UMLS found in config files. Looking in .env file.")
        try:
            apiKey = os.getenv("API_KEY_UMLS")
            print("Using API_KEY_UMLS found in .env file.")
            cfg.apis.user_info.apiKey = apiKey
            cfg.apis.query_params.apiKey = apiKey
        except ValueError:
            print(
                "No API_KEY_UMLS in .env file. Please add your UMLS API key to configs.apis.config_umls_api.yaml OR .env file."
            )
            exit()
    return cfg


@task(name="Connect to UMLS API")
def connect_to_umls(cfg):
    """Connects to UMLS API and prints response text"""

    payload = f"apikey={cfg.apis.user_info.apiKey}"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.request(
        "POST", cfg.apis.umls_api_settings.url, headers=headers, data=payload
    )
    print(response.text)
    return response
