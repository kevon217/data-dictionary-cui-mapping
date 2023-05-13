"""

This module contains functions for processing UMLS API queries.

"""

import json
import math

import pandas as pd
import requests
from prefect import flow, task


@task(name="Checking if query term is valid")
def check_query_terms_valid(query_term) -> bool:
    """Check if query term is valid"""

    if pd.isna(query_term) or len(query_term) == 0:
        valid = False
    else:
        valid = True
    return valid


@task(name="Returning INVALID QUERY TERM output")
def invalid_query_term_output(
    vn: str, searchID: int, searchTerm: str, searchTermCol: str
) -> list:
    """Row to add to results df if query term is invalid"""

    invalid_output = [
        vn,
        searchID,
        searchTerm,
        searchTermCol,
        "No search (value is nan or empty)",
        "No search (value is nan or empty)",
        "",
        "",
        "",
        "",
    ]
    return invalid_output


@task(name="Returning NO RESULTS output")
def no_results_output(
    vn: str, searchID: int, searchTerm: str, searchTermCol: str, searchType: str
) -> list:
    """Row to add to results df if no results"""

    results_none = [
        vn,
        searchID,
        searchTerm,
        searchTermCol,
        searchType,
        "NONE",
        "",
        "",
        "",
        "",
    ]
    return results_none


@task(name="Modifying UMLS query params dictionary")
def modify_query_params(query_params: dict, **kwargs) -> dict:
    """Create query dictionary for UMLS API search"""

    for key, value in kwargs.items():
        query_params[key] = value
    return query_params


def pages_to_view(recCount: int, cfg) -> list:
    """Return list of pages to query"""

    pages_max = cfg.apis.umls.api_settings.pages_max
    pageSize = cfg.apis.umls.api_settings.pageSize
    pages_idx = list(range(1, math.ceil(recCount / pageSize) + 1))
    pages = [page for page in pages_idx if page <= pages_max]
    return pages


@task(name="Querying UMLS api")
def query_umls_api(fullpath: str, query_params: dict) -> dict:
    """Query UMLS API and return results"""

    r = requests.get(fullpath, params=query_params)
    r.encoding = "utf-8"
    items = json.loads(r.text)
    jsonData = items["result"]
    return jsonData


@flow(flow_run_name="Processing UMLS query results")
def process_query_results(jsonData: dict, query_params: dict, cfg) -> pd.DataFrame:
    """Process query results"""

    results = pd.DataFrame()
    recCount = int(jsonData["recCount"])
    pages = pages_to_view(recCount, cfg)
    result_columns = cfg.custom.curation_settings.result_columns
    overall_rank = 0
    for pg in pages:
        if pg == 1:
            for i, item in enumerate(jsonData["results"]):
                results.loc[i, "recCount"] = recCount
                overall_rank += 1
                results.loc[i, "overall_rank"] = overall_rank
                results.loc[i, result_columns[2]] = item["name"]
                results.loc[i, result_columns[3]] = item["ui"]
                results.loc[i, result_columns[4]] = item["rootSource"]
        else:
            query_params["pageNumber"] = pg
            jsonData = query_umls_api(cfg.apis.umls.api_settings.fullpath, query_params)
            offset = results.shape[0]
            for i, item in enumerate(jsonData["results"]):
                i_offset = i + offset
                results.loc[i_offset, "recCount"] = recCount
                overall_rank += 1
                results.loc[i_offset, "overall_rank"] = overall_rank
                results.loc[i_offset, result_columns[2]] = item["name"]
                results.loc[i_offset, result_columns[3]] = item["ui"]
                results.loc[i_offset, result_columns[4]] = item["rootSource"]
    return results
