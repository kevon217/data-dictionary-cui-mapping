"""

Runner for iterating through queries stored in dataframe.

Used by batch_hybrid_query_pipeline.py to iterate through each row in the curation dataframe and query the UMLS API with each query term in the row and add to the df_results.

"""
import re

import pandas as pd
from prefect import flow
from tqdm import tqdm

from data_dictionary_cui_mapping.utils.text_processing import check_query_terms_valid
from . import umls_query_processing_functions as uqproc


@flow(flow_run_name="Running UMLS Runner")
def umls_runner(df_results, df_curation, cfg):
    cfg.apis.umls.query_params.apiKey = cfg.apis.umls.user_info.apiKey
    cnt_searchTerm = 0
    search_ID = 0
    for idx_row, row in tqdm(
        df_curation.iterrows(), total=df_curation.shape[0], desc="UMLS Runner"
    ):
        """
        Cycle through each row in df_curation and query UMLS API with each query term in the row.
        """
        search_ID += 1
        vn = row[cfg.custom.data_dictionary_settings.variable_column]  # variable name
        print(f"Querying search_ID [{search_ID}]: {vn}")
        query_terms_dict = {
            col: row[col]
            for col in df_curation.columns
            if re.search(r"query_term_\d+", col)
        }
        for key, val in query_terms_dict.items():
            cnt_searchTerm += 1
            searchTermCol = key
            searchTerm = val
            searchType = cfg.apis.umls.api_settings.searchType1
            pageNumber = 1
            if check_query_terms_valid(searchTerm):  # check if query term is valid
                query_params = uqproc.modify_query_params(
                    cfg.apis.umls.query_params,
                    string=searchTerm,
                    searchType=searchType,
                    pageNumber=pageNumber,
                )
                jsonData = uqproc.query_umls_api(
                    cfg.apis.umls.api_settings.fullpath, query_params
                )  # query API
                recCount = jsonData["recCount"]
                if (
                    recCount
                ):  # if recCount is not 0, results were found with default exact search
                    # print(
                    #     f"({cnt_searchTerm}) {searchTerm}: {recCount} {searchType} matches."
                    # )
                    df_results_cols = uqproc.process_query_results(
                        jsonData, query_params, cfg
                    )
                    df_query_cols = pd.DataFrame(
                        [[vn, search_ID, searchTerm, searchTermCol, searchType]]
                        * df_results_cols.shape[0],
                        columns=cfg.custom.curation_settings.query_columns,
                    )
                    df_temp = pd.concat([df_query_cols, df_results_cols], axis=1)
                    df_results = pd.concat([df_results, df_temp], ignore_index=True)
                    if cfg.custom.data_dictionary_settings.search_all_query_terms:
                        continue  # if search_all_cols is True, continue to next query term for the same row if it exists
                    else:
                        break  # if search_all_cols is False, break out of loop and move to next row
                else:  # for cases where the 'exact' search type results in an empty list
                    # print(
                    #     f"({cnt_searchTerm}) {searchTerm}: No exact match. Trying alternative searchType."
                    # )
                    temp_ls = uqproc.no_results_output(
                        vn, search_ID, searchTerm, searchTermCol, searchType
                    )
                    df_temp = pd.DataFrame(
                        dict(zip(df_results.columns, temp_ls)), index=[0]
                    )
                    df_results = pd.concat([df_results, df_temp], ignore_index=True)
                    searchType = (
                        cfg.apis.umls.api_settings.searchType2
                    )  # TODO: make stack to allow for iterating over multiple searchTypes
                    query_params = uqproc.modify_query_params(
                        cfg.apis.umls.query_params, searchType=searchType
                    )
                    jsonData = uqproc.query_umls_api(
                        cfg.apis.umls.api_settings.fullpath, query_params
                    )  # query API
                    recCount = jsonData["recCount"]
                    if (
                        recCount
                    ):  # if recCount is not 0, results were found with approximate search
                        cnt_searchTerm += 1
                        # print(
                        #     f"({cnt_searchTerm}) {searchTerm}: {recCount} {searchType} matches."
                        # )
                        df_results_cols = uqproc.process_query_results(
                            jsonData, query_params, cfg
                        )
                        df_query_cols = pd.DataFrame(
                            [[vn, search_ID, searchTerm, searchTermCol, searchType]]
                            * df_results_cols.shape[0],
                            columns=cfg.custom.curation_settings.query_columns,
                        )
                        df_temp = pd.concat([df_query_cols, df_results_cols], axis=1)
                        df_results = pd.concat([df_results, df_temp], ignore_index=True)
                        if cfg.custom.data_dictionary_settings.search_all_query_terms:
                            continue
                        else:
                            break
                    else:  # if approximate search still results in nothing, try next query_term if available
                        # print(
                        #     f"({cnt_searchTerm}) {searchTerm}: No alternative searchType match. Moving on to next query term option if available."
                        # )
                        temp_ls = uqproc.no_results_output(
                            vn, search_ID, searchTerm, searchTermCol, searchType
                        )
                        df_temp = pd.DataFrame(
                            dict(zip(df_results.columns, temp_ls)), index=[0]
                        )
                        df_results = pd.concat([df_results, df_temp], ignore_index=True)
                        continue
            else:  # if query term is not valid, try next query term if available
                # print(
                #     f"({cnt_searchTerm}) {searchTerm}: Is nan or empty. Trying next query term option if available."
                # )
                results_ls = uqproc.invalid_query_term_output(
                    vn, search_ID, searchTerm, searchTermCol
                )
                df_temp = pd.DataFrame(
                    dict(zip(df_results.columns, results_ls)), index=[0]
                )
                df_results = pd.concat([df_results, df_temp], ignore_index=True)

    return df_results
