"""

Upsert UMLS embeddings and metadata into Pinecone for semantic_search.

"""

from prefect import flow
from tqdm import tqdm
import pandas as pd
from pathlib import Path

import data_dictionary_cui_mapping.utils.helper as helper
from data_dictionary_cui_mapping.utils.BatchGenerator import BatchGenerator
from data_dictionary_cui_mapping.semantic_search.utils.api_connection import (
    check_credentials,
    connect_to_pinecone,
)

cfg = helper.compose_config.fn(
    config_path="../configs",
    overrides=["semantic_search=embeddings", "apis=config_pinecone_api"],
)


@flow(
    flow_run_name="Upserting UMLS semantic_search embeddings and metadata to Pinecone",
    log_prints=True,
)
def upsert_umls(cfg, **kwargs):
    # LOAD PROCESSED UMLS EMBEDDINGS
    if "kwargs" in locals():
        df_umls_embeddings = kwargs.get("df_umls_embeddings")
        if df_umls_embeddings is None or df_umls_embeddings.empty:
            if cfg.semantic_search.upsert.filepath_processed:
                df_umls_embeddings = pd.read_pickle(
                    cfg.semantic_search.upsert.filepath_processed
                )
            else:
                fp_umls_embeddings = helper.choose_file(
                    "Choose df_UMLS_embeddings.pkl file"
                )
                df_umls_embeddings = pd.read_pickle(fp_umls_embeddings)
                print(f"UMLD embeddings dataframe size is: {df_umls_embeddings.shape}")
                cfg.semantic_search.upsert.filepath_processed = fp_umls_embeddings

    # CONNECT TO PINECONE
    cfg = check_credentials.fn(cfg)
    pinecone = connect_to_pinecone.fn(cfg)
    print(
        f"Pinecone indexes available: {pinecone.list_indexes()}"
    )  # List all indexes currently present for your key

    # CHECK WHETHER THE INDEX WITH THE SAME NAME ALREADY EXISTS
    if cfg.semantic_search.pinecone.index.index_name not in pinecone.list_indexes():
        pinecone.create_index(
            name=cfg.semantic_search.pinecone.index.index_name,
            dimension=cfg.semantic_search.pinecone.index.dimension,
            metric=cfg.semantic_search.pinecone.index.metric,
            pod_type=cfg.semantic_search.pinecone.index.pod_type,
        )

    # INSERT UMLS VECTOR SEMANTIC_SEARCH INTO PINECONE INDEX
    index = pinecone.Index(index_name=cfg.semantic_search.pinecone.index.index_name)
    print(
        f"Stats for index '{cfg.semantic_search.pinecone.index.index_name}': {index.describe_index_stats()}"
    )

    # UPSERT EMBEDDINGS AND METADATA
    df_batcher = BatchGenerator(100)
    for col in cfg.semantic_search.upsert.embed_columns:
        print(f"Uploading vectors to {col} namespace..")
        for batch_df in tqdm(df_batcher(df_umls_embeddings)):
            vectors = []
            for i in range(len(batch_df)):
                vectors.append(
                    {
                        "id": batch_df.vector_id.iloc[i],
                        "values": batch_df[f"{col}_dense_vecs"].iloc[i],
                        "sparse_values": batch_df[f"{col}_sparse_vecs_upsert"].iloc[i],
                        "metadata": {
                            "STR_tokens": batch_df.STR_tokens.iloc[i],
                            "DEF_tokens": batch_df.DEF_tokens.iloc[i],
                            "CUI": batch_df.CUI.iloc[i],
                            "STR": batch_df.STR.iloc[i],
                            "SAB_MRDEF": batch_df.SAB_MRDEF.iloc[i],
                            "SAB_CUI_DEF_all": batch_df.SAB_CUI_DEF_all.iloc[i],
                            "SAB_MRCONSO": batch_df.SAB_MRCONSO.iloc[i],
                            "SAB_CUI_CONSO_all": batch_df.SAB_CUI_CONSO_all.iloc[i],
                            "STY": batch_df.STY.iloc[i],
                        },
                    }
                )
            index.upsert(vectors=vectors, namespace=col)

    # CHECK INDEX SIZE FOR EACH NAMESPACE
    print("Index size after upsert:")
    index.describe_index_stats()

    # SAVE CONFIG
    helper.save_config(
        cfg,
        Path(cfg.semantic_search.upsert.filepath_processed).parent,
        "config_upsert.yaml",
    )

    return index, cfg


if __name__ == "__main__":
    index, cfg = upsert_umls(cfg)
