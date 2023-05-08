"""

Main script for creating curation file for examples dictionary --> UMLS CUI mappings via MetaMap API.

"""
import sys
from prefect import flow
from prefect.task_runners import SequentialTaskRunner
from pathlib import Path
import data_dictionary_cui_mapping.utils.helper as helper
import data_dictionary_cui_mapping.utils.process_data_dictionary as proc_dd

# MetaMap API
from data_dictionary_cui_mapping.curation.utils import curation_functions as cur
from data_dictionary_cui_mapping.metamap.utils.api_connection import check_credentials
from data_dictionary_cui_mapping.metamap.utils import (
    metamap_query_processing_functions as mm_qproc,
)

cfg = helper.compose_config.fn(
    config_path="../configs",
    config_name="config",
    overrides=["custom=de", "apis=config_metamap_api"],
)


# @hydra.main(version_base=None, config_path="../configs", config_name="config")
@flow(
    flow_run_name="MetaMap search - batch_query_pipeline",
    log_prints=True,
    task_runner=SequentialTaskRunner(),
)
def run_mm_batch(cfg, **kwargs):
    # API CONNECTION
    cfg = check_credentials(cfg)

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
                f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_metamap-search"
            )
        )
    if df_dd_preprocessed is None or df_dd_preprocessed.empty:
        # PREPROCESS DATA DICTIONARY FILE
        df_dd_preprocessed = proc_dd.process_data_dictionary(df_dd, cfg)

    # FORMAT CURATION DATAFRAME
    if not cfg.custom.settings.pipeline_name:
        cfg.custom.settings.pipeline_name = (
            f"metamap (custom={cfg.custom.settings.custom_config})"
        )
    df_curation = cur.format_curation_dataframe(
        df_dd, df_dd_preprocessed, cfg.custom.settings.pipeline_name, cfg
    )

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
        print("MetaMap batch query pipeline failed!!!")
        sys.exit()

    # CREATE CURATION FILE
    df_final = cur.create_curation_file(
        dir_step1, df_dd, df_dd_preprocessed, df_curation, df_results, cfg
    )
    helper.save_config(cfg, dir_step1)
    print("FINISHED MetaMap batch query pipeline!!!")

    return df_final


if __name__ == "__main__":
    df_final = run_mm_batch(cfg)
