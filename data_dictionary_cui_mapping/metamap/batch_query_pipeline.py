"""

Main script for creating curation file for examples dictionary --> UMLS CUI mappings via MetaMap API.

"""


import os

from prefect import flow

import data_dictionary_cui_mapping.utils.helper as helper
import data_dictionary_cui_mapping.utils.process_data_dictionary as proc_dd
from data_dictionary_cui_mapping.metamap.utils import (
    metamap_query_processing_functions as mm_qproc,
)
from data_dictionary_cui_mapping.metamap.utils.api_connection import check_credentials
from data_dictionary_cui_mapping.curation.utils import curation_functions as cur

cfg = helper.compose_config.fn(
    config_path="../configs",
    config_name="config",
    overrides=["custom=de", "apis=config_metamap_api"],
)


# @hydra.main(version_base=None, config_path="../configs", config_name="config")
@flow(flow_run_name="MetaMap search - batch_query_pipeline", log_prints=True)
def main(cfg):
    # API CONNECTION
    cfg = check_credentials(cfg)

    # LOAD DATA DICTIONARY FILE
    df_dd, fp_dd = proc_dd.load_data_dictionary(cfg)

    # CREATE STEP 1 DIRECTORY
    dir_step1 = helper.create_folder(
        f"{os.path.dirname(fp_dd)}/{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_metamap-search"
    )

    # PREPROCESS DATA DICTIONARY FILE
    df_dd_preprocessed = proc_dd.process_data_dictionary(df_dd, cfg)

    # FORMAT CURATION DATAFRAME
    df_curation = cur.format_curation_dataframe(df_dd, df_dd_preprocessed, cfg)

    # FORMAT METAMAP BATCH INPUT
    df_mm_input = mm_qproc.format_for_metamap(df_curation, cfg)
    fp_mm_inputfile = mm_qproc.create_mm_inputfile(df_mm_input, dir_step1)

    # RUN METAMAP BATCH
    response = mm_qproc.run_batch_metamap_api(fp_mm_inputfile, cfg)

    # FORMAT METAMAP OUTPUT
    if response.status_code == 200:
        mm_json = mm_qproc.mm_output_to_json(response)
        fp_mm_json = mm_qproc.save_mm_output_json(mm_json, dir_step1)
        df_results = mm_qproc.process_mm_json_to_df(mm_json, cfg)
        df_results = mm_qproc.rename_mm_columns(df_results, cfg)
    else:
        print(response.text)
        exit()

    # CREATE CURATION FILE
    df_final = cur.create_curation_file(
        dir_step1, df_dd, df_dd_preprocessed, df_curation, df_results, cfg
    )

    helper.save_config(dir_step1, cfg)

    print("FINISHED MetaMap batch query pipeline!!!")

    return df_final


if __name__ == "__main__":
    df_final = main(cfg)
