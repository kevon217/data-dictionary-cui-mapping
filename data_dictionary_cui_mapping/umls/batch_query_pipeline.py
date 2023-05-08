"""

Main script for creating curation file for examples dictionary --> UMLS CUI mappings via UMLS API.

"""

import pandas as pd
from prefect import flow
from pathlib import Path

import data_dictionary_cui_mapping.utils.helper as helper
import data_dictionary_cui_mapping.utils.process_data_dictionary as proc_dd
import data_dictionary_cui_mapping.curation.utils.curation_functions as cur

# UMLS API
from data_dictionary_cui_mapping.umls.utils.api_connection import (
    check_credentials,
    connect_to_umls,
)
from data_dictionary_cui_mapping.umls.utils.runner import umls_runner

cfg = helper.compose_config.fn(overrides=["custom=pvd", "apis=config_umls_api"])


# @hydra.main(version_base=None, config_path="../configs", config_name="config")
@flow(flow_run_name="UMLS API Search - batch_query_pipeline", log_prints=True)
def run_umls_batch(cfg, **kwargs):
    # API CONNECTION
    check_credentials(cfg)
    response = connect_to_umls(cfg)

    # INPUTS/OUTPUTS
    df_dd = kwargs.get("df_dd")
    dir_step1 = kwargs.get("dir_step1")
    df_dd_preprocessed = kwargs.get("df_dd_preprocessed")
    if df_dd is None or df_dd.empty:
        # LOAD DATA DICTIONARY FILE
        df_dd, fp_dd = proc_dd.load_data_dictionary(cfg)
    if dir_step1 is None:
        # CREATE STEP 1 DIRECTORY
        dir_step1 = helper.create_folder(
            Path(fp_dd).parent.joinpath(
                f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_umls-api-search"
            )
        )
    if df_dd_preprocessed is None or df_dd_preprocessed.empty:
        # PREPROCESS DATA DICTIONARY FILE
        df_dd_preprocessed = proc_dd.process_data_dictionary(df_dd, cfg)

    # FORMAT CURATION DATAFRAME
    if not cfg.custom.settings.pipeline_name:
        cfg.custom.settings.pipeline_name = (
            f"umls (custom={cfg.custom.settings.custom_config})"
        )
    df_curation = cur.format_curation_dataframe(
        df_dd, df_dd_preprocessed, cfg.custom.settings.pipeline_name, cfg
    )

    # PREPARE UMLS QUERY RESULTS DATAFRAME FORMAT
    df_results = pd.DataFrame(
        columns=cfg.custom.curation_settings.query_columns
        + cfg.custom.curation_settings.result_columns
    )

    # RUN UMLS API SEARCH
    df_results = umls_runner(df_results, df_curation, cfg)

    # CREATE CURATION FILE
    df_final = cur.create_curation_file(
        dir_step1, df_dd, df_dd_preprocessed, df_curation, df_results, cfg
    )

    helper.save_config(cfg, dir_step1)

    print("FINISHED UMLS API batch query pipeline!!!")

    return df_final


if __name__ == "__main__":
    df_final = run_umls_batch(cfg)
