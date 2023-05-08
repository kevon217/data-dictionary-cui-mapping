"""

Functions to assist with tokenizing, embedding, and formatting data for upsert into Pinecone.

"""

from prefect import flow, task
from sentence_transformers import SentenceTransformer
from splade.models.transformer_rep import Splade
from transformers import AutoTokenizer
import torch

from data_dictionary_cui_mapping.semantic_search.utils.checks import (
    normalize_unit_length,
)


@task(name="Checking/Setting Device for embedding")
def check_set_device(cfg):
    """Check device settings to run on GPU if available, else cpu."""

    device = "cuda" if torch.cuda.is_available() else "cpu"
    cfg.semantic_search_settings.device = device
    print(f"Running on {device}")


@task(name="Adding Vector ID to Dataframe")
def add_vector_id(df):
    """Add vector_id column to dataframe for upsert into Pinecone."""

    df.insert(0, "vector_id", df.index)
    df["vector_id"] = df["vector_id"].apply(str)
    return df


@task(name="Adding Metadata to Dataframe")
def add_metadata(df, columns):  # TODO: figure out inputs
    """Add metadata columns to dataframe for upsert into Pinecone."""

    metadata = {}
    for col in columns:
        metadata[col] = df[col].values.tolist()
    if "metadata" not in df.columns:
        df["metadata"] = df.apply(
            lambda x: {col: x[col] for col in metadata.keys()}, axis=1
        )
    else:
        temp = df.apply(lambda x: {col: x[col] for col in metadata.keys()}, axis=1)
        df.apply(lambda x: x["metadata"].update(temp[int(x["vector_id"])]), axis=1)
    return df


@task(name="Tokenizing Columns and Adding to Metadata")
def tokenize_columns(df, columns, model_name):
    """Tokenize columns and add to metadata for upsert into Pinecone."""

    metadata = {}
    tokenizer = AutoTokenizer.from_pretrained(model_name, truncation=True)
    for col in columns:
        print(f"Tokenizing {col}")
        batch = df[col].values.tolist()
        tokens = [tokenizer.tokenize(sentence.lower()) for sentence in batch]
        df[f"{col}_tokens"] = tokens
        metadata[f"{col}_tokens"] = tokens
    if "metadata" not in df.columns:
        df["metadata"] = df.apply(
            lambda x: {col: x[col] for col in metadata.keys()}, axis=1
        )
    else:
        temp = df.apply(lambda x: {col: x[col] for col in metadata.keys()}, axis=1)
        df.apply(lambda x: x["metadata"].update(temp[int(x["vector_id"])]), axis=1)

    return df


@flow(
    flow_run_name="Building dense and sparse embeddings and adding metadata for upsert into Pinecone"
)
def hybrid_builder(
    df, embed_columns, dense_model_id, sparse_model_id, sparse_batch_size, cfg
):
    """Builds dense and sparse embeddings and adds metadata for upserting into Pinecone for hybrid semantic search"""

    dense_model = SentenceTransformer(
        dense_model_id, device=cfg.semantic_search_settings.device
    )

    tokenizer = AutoTokenizer.from_pretrained(sparse_model_id)
    sparse_model = Splade(sparse_model_id, agg="max")
    sparse_model.to(cfg.semantic_search_settings.device)  # move to GPU if possible
    idx2token = {idx: token for token, idx in tokenizer.get_vocab().items()}
    for col in embed_columns:
        print(f"Embedding {col}")
        batch = df[col].values.tolist()
        dense_vecs = dense_model.encode(
            batch,
            show_progress_bar=True,
            normalize_embeddings=cfg.upsert.embed.dense.normalize,
        )
        df[f"{col}_dense_vecs"] = dense_vecs.tolist()
        sparse_upsert = []
        sparse_idx2token = []
        for i in range(0, len(batch), sparse_batch_size):
            print(f"Embedding {i} to {i + sparse_batch_size}")
            batch_splade = batch[i : i + sparse_batch_size]
            input_ids = tokenizer(
                batch_splade, return_tensors="pt", padding=True, truncation=True
            )
            with torch.no_grad():
                sparse_vecs = sparse_model(
                    d_kwargs=input_ids.to(cfg.semantic_search_settings.device)
                )["d_rep"].squeeze()
            for id, sparse_vec in enumerate(sparse_vecs):
                indices = sparse_vec.nonzero().squeeze().cpu().tolist()  # positions
                values = sparse_vec[indices].cpu().tolist()  # weights/scores
                if cfg.upsert.embed.sparse.normalize:
                    values = normalize_unit_length(values)
                values, indices = zip(
                    *[(x, y) for x, y in sorted(zip(values, indices), reverse=True)]
                )
                values = list(values)
                indices = list(indices)
                sparse_dict = {
                    idx2token[idx]: round(weight, 2)
                    for idx, weight in zip(indices, values)
                }
                sparse_values = {"indices": indices, "values": values}
                sparse_upsert.append(sparse_values)
                sparse_idx2token.append(sparse_dict)
        df[f"{col}_sparse_vecs_upsert"] = sparse_upsert
        df[f"{col}_sparse_vecs_idx2token"] = sparse_idx2token
    return df


# # @task(name="Creating Dense Embeddings and Adding to Dataframe")
# def create_sentence_embeddings(df, model_name, columns, cfg):
#     """Create dense embeddings for each column in dataframe."""
#
#     dense_model = SentenceTransformer(model_name, device=cfg.semantic_search_settings.device)
#     for col in columns:
#         print(f"Embedding {col}")
#         batch = df[col].values.tolist()
#         embedding = dense_model.encode(batch, show_progress_bar=True)
#         df[f"{col}_dense_vector"] = embedding.tolist()
#     return df


# TODO: create generic dense and sparse functions that take in package function and model id and return embeddings
