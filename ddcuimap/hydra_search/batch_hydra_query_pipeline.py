"""

Batch query pipeline with UMLS API, MetaMap API, and Semantic Search

"""

import pandas as pd
from pathlib import Path

from ddcuimap.hydra_search import hydra_logger, log, copy_log
import ddcuimap.utils.helper as helper
import ddcuimap.curation.utils.process_data_dictionary as proc_dd
import ddcuimap.curation.utils.curation_functions as cur
import ddcuimap.umls.batch_query_pipeline as umls
import ddcuimap.metamap.batch_query_pipeline as mm
import ddcuimap.semantic_search.batch_hybrid_query_pipeline as ss

cfg_hydra = helper.compose_config(overrides=["custom=hydra_base"])
cfg_umls = helper.compose_config(overrides=["custom=de", "apis=config_umls_api"])
cfg_mm = helper.compose_config(overrides=["custom=de", "apis=config_metamap_api"])
cfg_ss = helper.compose_config(
    overrides=[
        "custom=title_def",
        "semantic_search=embeddings",
        "apis=config_pinecone_api",
    ]
)


@log(msg="Running UMLS/MetaMap/Semantic Search hydra search pipeline")
def run_hydra_batch(cfg_hydra, **kwargs):
    # LOAD DATA DICTIONARY FILE
    df_dd, fp_dd = proc_dd.load_data_dictionary(cfg_hydra)

    # CREATE STEP 1 DIRECTORY
    dir_step1 = helper.create_folder(
        Path(fp_dd).parent.joinpath(
            f"{cfg_hydra.custom.curation_settings.file_settings.directory_prefix}_Step-1_Hydra-search"
        )
    )

    # STORE PIPELINE RESULTS
    cat_dfs = []

    ## UMLS API ##
    cfg_umls = kwargs.get("cfg_umls")
    if cfg_umls:
        dir_step1_umls = helper.create_folder(
            Path(dir_step1).joinpath(
                f"{cfg_hydra.custom.curation_settings.file_settings.directory_prefix}_Step-1_umls-api-search"
            )
        )
        df_umls, cfg_umls = umls.run_umls_batch(
            cfg_umls, df_dd=df_dd, dir_step1=dir_step1_umls
        )
        cat_dfs.append(df_umls)

    ## METAMAP API ##
    cfg_mm = kwargs.get("cfg_mm")
    if cfg_mm:
        dir_step1_mm = helper.create_folder(
            Path(dir_step1).joinpath(
                f"{cfg_hydra.custom.curation_settings.file_settings.directory_prefix}_Step-1_metamap-search"
            )
        )
        df_metamap, cfg_mm = mm.run_mm_batch(
            cfg_mm, df_dd=df_dd, dir_step1=dir_step1_mm
        )
        cat_dfs.append(df_metamap)

    ## SEMANTIC SEARCH ##
    cfg_ss = kwargs.get("cfg_ss")
    if cfg_ss:
        dir_step1_ss = helper.create_folder(
            Path(dir_step1).joinpath(
                f"{cfg_hydra.custom.curation_settings.file_settings.directory_prefix}_Step-1_hybrid-semantic-search_alpha={cfg_ss.semantic_search.query.alpha}"
            )
        )
        df_semantic_search, cfg_ss = ss.run_hybrid_ss_batch(
            cfg_ss, df_dd=df_dd, dir_step1=dir_step1_ss
        )
        cat_dfs.append(df_semantic_search)

    ## COMBINE RESULTS ##

    df_results = pd.concat(cat_dfs, axis=0, ignore_index=True)
    df_results.to_csv(Path(dir_step1).joinpath("hydra_search_results.csv"), index=False)

    # FORMAT CURATION DATAFRAME
    df_dd_preprocessed = proc_dd.process_data_dictionary(df_dd, cfg_hydra)
    pipeline_name = f"hydra-search (custom={cfg_hydra.custom.settings.custom_config})"
    df_curation = cur.format_curation_dataframe(
        df_dd, df_dd_preprocessed, pipeline_name, cfg_hydra
    )
    curation_cols = list(cfg_hydra.custom.curation_settings.information_columns) + [
        "search_ID"
    ]
    df_curation = df_curation[curation_cols]

    ## CREATE CURATION FILE ##
    df_final = cur.create_curation_file(
        dir_step1, df_dd, df_dd_preprocessed, df_curation, df_results, cfg_hydra
    )

    hydra_logger.info("FINISHED batch hydra search query pipeline!!!")

    # SAVE CONFIG FILE AND MOVE LOG
    helper.save_config(cfg_hydra, dir_step1, "config_query.yaml")
    copy_log(hydra_logger, dir_step1, "hydra_logger.log")

    return df_final, cfg_hydra


if __name__ == "__main__":
    df_final, cfg_hydra = run_hydra_batch(
        cfg_hydra, cfg_umls=cfg_umls, cfg_mm=cfg_mm, cfg_ss=cfg_ss
    )  # TODO: maybe put module cfgs into a list
