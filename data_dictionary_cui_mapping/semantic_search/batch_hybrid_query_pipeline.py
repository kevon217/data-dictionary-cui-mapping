"""

Main script for creating curation file based off semantic search of UMLS embeddings in Pinecone .

"""

import importlib.resources
import pickle

import pandas as pd

# import json
from prefect import flow
from pathlib import Path

import data_dictionary_cui_mapping.utils.helper as helper
import data_dictionary_cui_mapping.utils.process_data_dictionary as proc_dd
import data_dictionary_cui_mapping.curation.utils.curation_functions as cur

# Semantic Search with Pinecone
from data_dictionary_cui_mapping.semantic_search.utils.api_connection import (
    check_credentials,
    connect_to_pinecone,
)

from data_dictionary_cui_mapping.semantic_search.utils import builders
from data_dictionary_cui_mapping.semantic_search.utils import runners as run

cfg = helper.compose_config.fn(
    overrides=[
        "custom=title_def",
        "apis=config_pinecone_api",
        "semantic_search=embeddings",
    ]
)


# @hydra.main(version_base=None, config_path="../configs", config_name="config")
@flow(
    flow_run_name="Pinecone Semantic Search - batch_hybrid_query_pipeline",
    log_prints=True,
)
def run_hybrid_ss_batch(cfg, **kwargs):
    # CONNECT TO PINECONE
    cfg = check_credentials(cfg)
    pinecone = connect_to_pinecone(cfg)
    print(
        f"Pinecone indexes available: {pinecone.list_indexes()}"
    )  # List all indexes currently present for your key
    index = pinecone.Index(cfg.semantic_search.pinecone.index.index_name)
    print(
        f"Stats for index '{cfg.semantic_search.pinecone.index.index_name}': {index.describe_index_stats()}"
    )

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
                f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-1_hybrid-semantic-search"
            )
        )
    if df_dd_preprocessed is None or df_dd_preprocessed.empty:
        # PREPROCESS DATA DICTIONARY FILE
        df_dd_preprocessed = proc_dd.process_data_dictionary(df_dd, cfg)

    # FORMAT CURATION DATAFRAME
    if not cfg.custom.settings.pipeline_name:
        cfg.custom.settings.pipeline_name = f"hybrid_semantic_search (custom={cfg.custom.settings.custom_config}, alpha={cfg.semantic_search.query.alpha})"
    df_curation = cur.format_curation_dataframe(
        df_dd, df_dd_preprocessed, cfg.custom.settings.pipeline_name, cfg
    )

    # BATCH EMBED QUERY TERMS
    if (
        cfg.semantic_search.query.filepath_embeddings
        and Path(cfg.semantic_search.query.filepath_embeddings).exists()
    ):  # TODO: modify to for loop to allow for list of alphas
        df_query_embeddings = pd.read_pickle(
            cfg.semantic_search.query.filepath_embeddings
        )
    else:
        builders.check_set_device(cfg.semantic_search)
        df_query_embeddings = df_curation.pipe(  # df_curation.copy() will leave embeddings out of df_curation and df_final
            builders.hybrid_builder,
            embed_columns=cfg.semantic_search.query.embed_columns,
            dense_model_id=cfg.semantic_search.query.embed.dense.model_name,
            sparse_model_id=cfg.semantic_search.query.embed.sparse.model_name,
            sparse_batch_size=cfg.semantic_search.query.embed.sparse.batch_size,
            cfg=cfg.semantic_search,
        ).pipe(
            builders.tokenize_columns,
            cfg.semantic_search.query.metadata.tokenize_columns,
            cfg.semantic_search.query.metadata.tokenizer.model_name,
        )
        df_query_embeddings.to_pickle(Path(dir_step1, "df_query_embeddings.pkl"))
        cfg.semantic_search.query.filepath_embeddings = str(
            Path(
                dir_step1
                / f"df_query_embeddings_alpha={cfg.semantic_search.query.alpha}.pkl"
            ).resolve()
        )

    # RETRIEVE UMLS VECTOR ID AS DICTIONARY
    ids = importlib.resources.read_binary(
        "semantic_search.resources", "dict_umls_upsert_ids.pkl"
    )
    dict_umls_upsert_ids = pickle.loads(ids)
    # dict_umls_upsert_ids = run.fetch_id_metadata(index, cfg) #TODO: need to work on this

    # RUN BATCH QUERY
    var_results = run.hybrid_search_runner(
        df_query_embeddings, cfg.semantic_search.query.alpha, cfg
    )
    # fp_var_results = Path(dir_step1, f"var_results_{ cfg.custom.settings.pipeline_name}.json") # TODO: fix this to serialize QueryResponseObject to json
    # with open(fp_var_results, "w") as f:
    #     json.dump(var_results, f)

    # AGGREGATE AND RANK RESULTS
    df_agg = run.aggregate_results(var_results, dict_umls_upsert_ids, cfg)
    df_agg = df_agg.rename(
        columns={
            "cui": "data element concept identifiers",
            "title": "data element concept names",
            "title_source": "data element terminology sources",
        }
    )
    df_agg.insert(2, "recCount", cfg.semantic_search.query.top_k)

    # CREATE CURATION FILE
    df_final = cur.create_curation_file(
        dir_step1, df_dd, df_dd_preprocessed, df_curation, df_agg, cfg
    )  # TODO: may want to include sparse tokens and scoring in curation file

    helper.save_config(cfg, dir_step1, "config_query.yaml")

    print("FINISHED Pinecone Semantic Search batch query pipeline!!!")

    return df_final, cfg


if __name__ == "__main__":
    df_final, cfg = run_hybrid_ss_batch(cfg)
