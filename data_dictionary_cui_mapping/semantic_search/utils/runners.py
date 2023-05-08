"""

This module contains custom defined runners for semantic search.

"""

import pinecone
import pandas as pd
from tqdm import tqdm
from prefect import task, flow

# from data_dictionary_cui_mapping.utils.timer import timer


def fetch_id_metadata(index, cfg):
    """Fetch metadata for a list of IDs from Pinecone index"""

    namespaces_cfg = cfg.semantic_search.pinecone.index.namespaces
    namespaces_index = index.describe_index_stats()["namespaces"].keys()
    namespaces = list(set(namespaces_cfg).intersection(namespaces_index))

    dict_ids_upsert = {}
    for name in namespaces:
        vector_count = index.describe_index_stats()["namespaces"][name]["vector_count"]
        ids = list(map(str, range(vector_count)))
        fetch_response = index.fetch(ids=ids, namespace=name)
        dict_ids_upsert[name] = fetch_response.ids
    return dict_ids_upsert


# @task(name="Scaling Dense and Sparse Vectors")
def hybrid_scale(dense, sparse, alpha: float):
    """Scale dense and sparse vectors for hybrid search"""

    # check alpha value is in range
    if alpha < 0 or alpha > 1:
        raise ValueError("Alpha must be between 0 and 1")
    # scale sparse and dense vectors to create hybrid search vecs
    hsparse = {
        "indices": sparse["indices"],
        "values": [v * (1 - alpha) for v in sparse["values"]],
    }
    hdense = [v * alpha for v in dense]
    return hdense, hsparse


# @flow(flow_run_name="Running Pinecone Semantic Search Runner")
# @timer
def hybrid_search_runner(df_embeddings, alpha, cfg):  # TODO: add alpha to config
    """Custom defined query runner for semantic search"""

    index_name = cfg.semantic_search.pinecone.index.index_name
    index = pinecone.Index(index_name=index_name)
    var_results = {}
    for e, (_, query_row) in tqdm(
        enumerate(df_embeddings.iterrows()),
        total=df_embeddings.shape[0],
        desc="Semantic Search Runner",
    ):
        vn = query_row.get(cfg.custom.data_dictionary_settings.variable_column)
        search_ID = query_row.get("search_ID")
        results = {}
        # RUN DEFINED QUERIES
        for colname, query in cfg.semantic_search.query.queries.hybrid.items():
            col_prefix = query[0]
            namespace = query[1]
            dense_vec = query_row[f"{col_prefix}_dense_vecs"]
            sparse_vec = query_row[
                f"{col_prefix}_sparse_vecs_upsert"
            ]  # TODO need to specify query instead of upsert
            dense_vec, sparse_vec = hybrid_scale(dense_vec, sparse_vec, alpha)
            results[colname] = index.query(
                namespace=namespace,
                top_k=cfg.semantic_search.query.top_k,
                vector=dense_vec,
                sparse_vector=sparse_vec,
                include_metadata=True,
            )
        var_results[vn] = {search_ID: results}
    return var_results


# add timer decorator
# @task(name="Aggregating Pinecone Semantic Search Results")
# @timer
def aggregate_results(var_results, dict_ids, cfg):
    """Aggregates query results"""

    df_agg = pd.DataFrame()
    for var in tqdm(
        var_results.keys(), total=len(var_results.keys()), desc="Aggregating Results"
    ):
        overall_query_scores = {}
        overall_query_count = {}
        query_rank = {}
        query_score = {}
        search_IDs = var_results[var]
        for search_ID in search_IDs:
            results = search_IDs[search_ID]
            for query_key in results.keys():
                ids = [r.id for r in results[query_key].matches]
                scores = [r.score for r in results[query_key].matches]
                query_rank[query_key] = {}
                query_score[query_key] = {}
                for rank, (id, score) in enumerate(zip(ids, scores)):
                    query_rank[query_key].update({id: int(rank + 1)})
                    query_score[query_key].update({id: score})
                for id, score in zip(ids, scores):
                    if id not in overall_query_scores:
                        overall_query_scores[id] = score
                        overall_query_count[id] = 1
                    else:
                        overall_query_scores[id] += score
                        overall_query_count[id] += 1
            df_temp = pd.DataFrame(
                {
                    "result_id": overall_query_scores.keys(),
                    "title": [
                        dict_ids[_id].get("STR") if _id in dict_ids else " "
                        for _id in overall_query_scores.keys()
                    ],
                    "definition": [
                        dict_ids[_id].get("DEF") if _id in dict_ids else " "
                        for _id in overall_query_scores.keys()
                    ],
                    "cui": [
                        dict_ids[_id].get("CUI") if _id in dict_ids else " "
                        for _id in overall_query_scores.keys()
                    ],
                    "semantic_type": [
                        dict_ids[_id].get("STY") if _id in dict_ids else " "
                        for _id in overall_query_scores.keys()
                    ],
                    "title_source": [
                        dict_ids[_id].get("SAB_MRCONSO") if _id in dict_ids else " "
                        for _id in overall_query_scores.keys()
                    ],
                    "definition_source": [
                        dict_ids[_id].get("SAB_MRDEF") if _id in dict_ids else " "
                        for _id in overall_query_scores.keys()
                    ],
                    "overall_count": overall_query_count.values(),
                    "average_score": [
                        round(r / c, 3)
                        for r, c in zip(
                            overall_query_scores.values(), overall_query_count.values()
                        )
                    ],
                }
            )
            df_temp.insert(0, cfg.custom.data_dictionary_settings.variable_column, var)
            df_temp.insert(1, "search_ID", search_ID)
            df_temp.sort_values(
                by=["overall_count", "average_score"], ascending=False, inplace=True
            )
            df_temp["overall_rank"] = [
                i + 1 for i in range(len(df_temp))
            ]  # Rank by count and average score
            for query_key in results.keys():
                df_temp[f"{query_key}_rank"] = [
                    query_rank[query_key].get(_id) for _id in df_temp.result_id
                ]
                df_temp[f"{query_key}_rank"] = (
                    df_temp[f"{query_key}_rank"].fillna(0).astype("int")
                )  # Fill in missing values with 0 for rank
                df_temp[f"{query_key}_score"] = [
                    query_score[query_key].get(_id) for _id in df_temp.result_id
                ]  # TODO: need to figure out if index needs to be reset or doesn't matter
            if df_agg.empty:
                df_agg = df_temp.copy()
            else:
                df_agg = pd.concat([df_agg, df_temp], axis=0)
    return df_agg


# @task(name="Query Pinecone Index")
# def query_pinecone_index(
#     query_row: pd.Series, index_name: str, namespace: str, col_embed: str, top_k: int
# ):
#     index = pinecone.Index(index_name=index_name)
#     embedding = query_row[col_embed]
#     query_results = index.query(
#         embedding, namespace=namespace, top_k=top_k, include_metadata=True
#     )
#     return query_results

#
# @flow(flow_run_name="Running Pinecone Semantic Search Runner")
# def dense_search_runner(df_embeddings, cfg):
#     """Custom defined query runner for semantic search"""
#
#     index = cfg.apis.index_info.index
#     var_results = {}
#     for e, (_, query_row) in enumerate(df_embeddings.iterrows()):
#         vn = query_row.get(cfg.custom.data_dictionary_settings.variable_column)
#         search_ID = query_row.get("search_ID")
#         results = {}
#         # RUN DEFINED QUERIES
#         for colname, query in cfg.semantic_search.query.queries.items():
#             col_embed = query[0]
#             namespace = query[1]
#             results[colname] = query_pinecone_index(
#                 query_row,
#                 index_name=index,
#                 col_embed=col_embed,
#                 namespace=namespace,
#                 top_k=cfg.semantic_search.query.top_k,
#             )
#         var_results[vn] = {search_ID: results}
#     return var_results

# # @flow(flow_run_name="Running Semantic Search Evaluation Measures")
# def evaluation_measures(df_agg, df_reference_cuis, cfg):
#     # TODO: WRITE FUNCTION TO EVALUATE RESULTS BASED ON REFERENCE CUIs
#
#     return df_eval
