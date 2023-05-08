"""

Batch query pipeline with UMLS API, MetaMap API, and Semantic Search

"""

import pandas as pd
from prefect import flow
from pathlib import Path

import data_dictionary_cui_mapping.utils.helper as helper
import data_dictionary_cui_mapping.utils.process_data_dictionary as proc_dd
import data_dictionary_cui_mapping.curation.utils.curation_functions as cur
import data_dictionary_cui_mapping.umls.batch_query_pipeline as umls
import data_dictionary_cui_mapping.metamap.batch_query_pipeline as mm
import data_dictionary_cui_mapping.semantic_search.batch_hybrid_query_pipeline as ss

cfg = helper.compose_config.fn(overrides=["custom=hydra_base"])
cfg_umls = helper.compose_config.fn(overrides=["custom=de", "apis=config_umls_api"])
cfg_mm = helper.compose_config.fn(overrides=["custom=de", "apis=config_metamap_api"])
cfg_ss = helper.compose_config.fn(
    overrides=[
        "custom=title_def",
        "semantic_search=embeddings",
        "apis=config_pinecone_api",
    ]
)


# @hydra.main(version_base=None, config_path="../configs", config_name="config")
@flow(
    flow_run_name="Running UMLS/MetaMap/Semantic Search hydra search pipeline",
    log_prints=True,
)
def run_hydra_batch(cfg, cfg_umls, cfg_mm, cfg_ss, **kwargs):
    # LOAD DATA DICTIONARY FILE
    df_dd, fp_dd = proc_dd.load_data_dictionary(cfg)

    # CREATE STEP 1 DIRECTORY
    dir_step1 = helper.create_folder.fn(
        Path(fp_dd).parent.joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_Hydra-search"
        )
    )

    ## UMLS API ##
    dir_step1_umls = helper.create_folder(
        Path(dir_step1).joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_umls-api-search"
        )
    )
    df_umls = umls.run_umls_batch(cfg_umls, df_dd=df_dd, dir_step1=dir_step1_umls)

    ## METAMAP API ##
    dir_step1_mm = helper.create_folder(
        Path(dir_step1).joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_metamap-search"
        )
    )
    df_metamap = mm.run_mm_batch(cfg_mm, df_dd=df_dd, dir_step1=dir_step1_mm)

    ## SEMANTIC SEARCH ##
    ls_df_alphas = []
    alphas = cfg_ss.semantic_search.query.alpha
    if type(alphas) != list:
        alphas = [alphas]
    for alpha in alphas:
        dir_step1_ss = helper.create_folder(
            Path(dir_step1).joinpath(
                f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_hybrid-semantic-search_alpha={alpha}"
            )
        )
        cfg_ss.custom.settings.pipeline_name = f"hybrid_semantic_search (custom={cfg.custom.settings.custom_config}, alpha={alpha})"
        cfg_ss.semantic_search.query.alpha = alpha
        cfg_ss.semantic_search.query.filepath_embeddings = None
        df_run, cfg_ss = ss.run_hybrid_ss_batch(
            cfg_ss, df_dd=df_dd, dir_step1=dir_step1_ss
        )
        ls_df_alphas.append(df_run)
    df_semantic_search = pd.concat(ls_df_alphas, axis=0)
    cfg_ss.semantic_search.query.alpha = alphas
    dir_step1_ss_alphas = helper.create_folder(
        Path(dir_step1).joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_hybrid-semantic-search_alpha={alphas}"
        )
    )
    df_semantic_search.to_csv(
        Path(dir_step1_ss_alphas).joinpath("results.csv"), index=False
    )

    ## COMBINE RESULTS ##

    df_results = pd.concat(
        [df_umls, df_metamap, df_semantic_search], axis=0, ignore_index=True
    )
    df_results.to_csv(Path(dir_step1).joinpath("hydra_search_results.csv"), index=False)
    # df_results = pd.read_csv(Path(dir_step1).joinpath("hydra_search_results.csv"))

    # FORMAT CURATION DATAFRAME
    df_dd_preprocessed = proc_dd.process_data_dictionary(df_dd, cfg)
    pipeline_name = f"hydra-search (custom={cfg.custom.settings.custom_config})"
    df_curation = cur.format_curation_dataframe(
        df_dd, df_dd_preprocessed, pipeline_name, cfg
    )
    curation_cols = list(cfg.custom.curation_settings.information_columns) + [
        "search_ID"
    ]
    df_curation = df_curation[curation_cols]

    ## CREATE CURATION FILE ##
    df_final = cur.create_curation_file(
        dir_step1, df_dd, df_dd_preprocessed, df_curation, df_results, cfg
    )
    helper.save_config(cfg, dir_step1)
    print("FINISHED batch hydra search query pipeline!!!")

    return df_final


if __name__ == "__main__":
    df_final = run_hydra_batch(cfg, cfg_umls, cfg_mm, cfg_ss)
