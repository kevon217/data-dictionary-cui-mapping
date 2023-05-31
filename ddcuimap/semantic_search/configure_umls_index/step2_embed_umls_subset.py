"""

Embed UMLS subset before upsert into Pinecone vector database for semantic search.

"""

import pandas as pd
from pathlib import Path

import ddcuimap.utils.helper as helper
from ddcuimap.semantic_search import ss_logger, log
from ddcuimap.semantic_search.utils import builders

cfg = helper.compose_config(
    config_path="../configs/semantic_search", config_name="embeddings", overrides=[]
)


@log(msg="Creating UMLS concept semantic search embeddings")
def embed_umls(cfg, **kwargs):
    # LOAD UMLS CONCEPT DATAFRAME
    if "kwargs" in locals():
        df_umls = kwargs.get("df_umls")
        if df_umls is None or df_umls.empty:
            if cfg.upsert.filepath_raw:
                ss_logger.warning(
                    f"Using UMLS concept dataframe: {cfg.upsert.filepath_raw}"
                )
            else:
                cfg.upsert.filepath_raw = helper.choose_dir(
                    "Choose UMLS concept dataframe"
                )
            df_umls = pd.read_pickle(cfg.upsert.filepath_raw)

    # BUILD EMBEDDINGS
    builders.check_set_device(cfg)
    df_umls_embeddings = (
        df_umls.copy()
        .pipe(builders.add_vector_id)
        .pipe(
            builders.hybrid_builder,
            cfg.upsert.embed_columns,
            cfg.upsert.embed.dense.model_name,
            cfg.upsert.embed.sparse.model_name,
            cfg.upsert.embed.sparse.batch_size,
            cfg,
        )
        .pipe(builders.add_metadata, cfg.upsert.metadata.include_columns)
        .pipe(
            builders.tokenize_columns,
            cfg.upsert.metadata.tokenize_columns,
            cfg.upsert.metadata.tokenizer.model_name,
        )
    )

    # SAVE EMBEDDINGS
    df_umls_embeddings.to_pickle(cfg.upsert.filepath_processed)

    # SAVE CONFIG
    helper.save_config(
        cfg, Path(cfg.upsert.filepath_processed).parent, "config_embeddings.yaml"
    )

    return df_umls_embeddings, cfg


if __name__ == "__main__":
    df_umls_embeddings, cfg = embed_umls(cfg)
