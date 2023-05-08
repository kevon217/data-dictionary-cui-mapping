"""

Pipeline for:

1. Creating a subset of UMLS Metathesaurus
2. Embedding UMLS concepts
3. Upserting UMLS embeddings and metadata into Pinecone

"""

from prefect import flow
import data_dictionary_cui_mapping.utils.helper as helper
from semantic_search.configure_umls_index import (
    step1_select_umls_subset as step1,
    step2_embed_umls_subset as step2,
    step3_upsert_umls_subset as step3,
)

cfg = helper.compose_config.fn(
    overrides=[
        "semantic_search=embeddings",
        "apis=config_pinecone_api",
    ]
)


@flow(flow_run_name="Running UMLS subset/embed/upsert pipeline", log_prints=True)
def main_flow(cfg):
    """
    Main flow pipeline for creating:
     (1) subset of UMLS Metathesaurus
     (2) embedding UMLS concepts
     (3) upserting UMLS embeddings and metadata into Pinecone
    """

    df_umls, cfg_step1 = step1.subset_umls(cfg.semantic_search)
    df_umls_embeddings, cfg_step2 = step2.embed_umls(
        cfg.semantic_search, df_umls=df_umls
    )
    index, cfg_step3 = step3.upsert_umls(cfg, df_umls_embeddings)

    print("FINISHED UMLS SUBSET/EMBED/UPSERT batch query pipeline!!!")

    return df_umls, df_umls_embeddings, index, cfg


if __name__ == "__main__":
    df_umls, df_umls_embeddings, index, cfg = main_flow(cfg)
