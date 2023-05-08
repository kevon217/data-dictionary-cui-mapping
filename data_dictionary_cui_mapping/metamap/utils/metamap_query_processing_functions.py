"""

This module contains functions for processing query terms for MetaMap ingestion and processing output from MetaMap.

"""


import json
import re

import pandas as pd
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner

from data_dictionary_cui_mapping.metamap.skr_web_api import Submission
from data_dictionary_cui_mapping.utils.text_processing import (
    check_query_terms_valid,
    unescape_string,
)


@task(name="Formatting query terms for MetaMap")
def format_for_metamap(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Formats query term for MetaMap ingestion"""

    # TODO: figure out how to add cfg.data_dictionary_settings.search_all_query_term parameter
    query_terms_cols = [col for col in df.columns if re.search(r"query_term_\d+", col)]
    # for every row in df check if there is a valid value in one of the query_term_columns, if so combine the variable_column and the first valid query_term_column in MetaMap_input column, but if there's not leave MetaInput_column blank blank
    for index, row in df.iterrows():
        for col in query_terms_cols:
            if check_query_terms_valid(row[col]):
                df.loc[
                    index, "MetaMap_input"
                ] = f"{row[cfg.custom.data_dictionary_settings.variable_column]}_{row['search_ID']}|{row[col]}"
                break
            else:
                df.loc[index, "MetaMap_input"] = ""
    return df


@task(name="Creating MetaMap SinglinePMID.txt input file")
def create_mm_inputfile(df: pd.DataFrame, dir_step1):  # TODO: add file name as argument
    """Creates file for MetaMap input"""

    # create a new dataframe with only the MetaMap_input column but first remove rows that are null
    df_mm_inputfile = df[df["MetaMap_input"] != ""]
    df_mm_inputfile = df_mm_inputfile["MetaMap_input"]
    fp_mm_inputfile = f"{dir_step1}/metamap-search_inputfile_SingLinePMID.txt"
    with open(fp_mm_inputfile, "w") as f:
        for cell in df_mm_inputfile:
            f.write(cell)
            f.write("\n")
    return fp_mm_inputfile


def create_mm_command_args(cmdargs) -> str:
    """Creates MetaMap command arguments"""

    # join together all the values in a dictionary by a space
    cmdargs = " ".join([f"{value}" for key, value in cmdargs.items()])
    return cmdargs


@flow(flow_run_name="Running MetaMap batch query", task_runner=SequentialTaskRunner)
def run_batch_metamap_api(fp_mm_inputfile, cfg):
    """This function calls the MetaMap API."""

    inst = Submission(
        cfg.apis.metamap.user_info.email, cfg.apis.metamap.user_info.apiKey
    )
    cmd = cfg.apis.metamap.api_settings.cmd
    cmdargs = create_mm_command_args(cfg.apis.metamap.api_settings.cmdargs)
    inst.set_serviceurl(cfg.apis.metamap.api_settings.serviceurl)
    inst.init_generic_batch(cfg.apis.metamap.api_settings.cmd, unescape_string(cmdargs))
    inst.set_batch_file(fp_mm_inputfile)
    inst.form["SingLinePMID"] = "yes"
    inst.form["Batch_Command"] = "{} {}".format(cmd, unescape_string(cmdargs))
    print("MetaMap Batch in progress...")  # TODO: put in progress bar here
    response = inst.submit()
    # print("response status: {}".format(response.status_code))
    # print("content: {}".format(response.content.decode()))
    return response


@task(name="Converting MetaMap output to JSON")
def mm_output_to_json(response):
    """Decodes MetaMap output to JSON and removes 'NOT DONE LOOP' to process properly"""

    jsondata0 = response.content.decode()
    jsondata = jsondata0.replace(
        "NOT DONE LOOP", ""
    )  # remove string "NOT DONE LOOP" if present
    mm_json = json.loads(jsondata)
    # print("examples:{}".format(mm_json))
    return mm_json


@task(name="Saving MetaMap output as JSON")
def save_mm_output_json(data, dir_step1):
    """Saves MetaMap output as json file"""

    fp_json = f"{dir_step1}/metamap-search_output.json"
    with open(fp_json, "w") as f:
        json.dump(data, f)
    return fp_json


@task(name="Processing JSON conversion to dataframe")
def process_mm_json_to_df(mm_json, cfg) -> pd.DataFrame:
    """Processes MetaMap output json file and returns dataframe"""
    #
    # if fp_json: # TODO: add option to read in json file instead of dictionary
    #     with open(fp_json) as f:
    #         mm_json = json.load(f)

    dict_docs = mm_json["AllDocuments"]
    ls_df_maps = []
    for doc in dict_docs:
        pmid = doc["Document"]["Utterances"][0]["PMID"]
        search_ID = int(pmid.split("_")[-1])  # get search_ID from PMID
        df_temp = pd.json_normalize(
            doc["Document"]["Utterances"][0]["Phrases"],
            record_path=["Candidates"],
            errors="ignore",
        )
        if df_temp.empty:
            df_temp = pd.DataFrame(columns=cfg.apis.metamap.output_settings.columns)
        df_temp.insert(0, "PMID", pmid)
        df_temp.insert(1, "search_ID", search_ID)
        df_temp.insert(2, "recCount", len(df_temp))
        df_temp.insert(
            3, "overall_rank", rank_CandidateScore(df_temp["CandidateScore"])
        )
        ls_df_maps.append(df_temp)
    df = pd.concat(
        ls_df_maps
    )  # combine separate dataframes for each PMID into one dataframe

    return df


def rank_CandidateScore(CandidateScore):
    """Turn CandidateScore into overall_rank"""

    overall_rank = (
        CandidateScore.astype(int).rank(method="dense", ascending=True).astype(int)
    )

    return overall_rank


@task(name="Renaming MetaMap columns for curation file")
def rename_mm_columns(df: pd.DataFrame, cfg) -> pd.DataFrame:
    """Renames MetaMap output columns"""

    rename_dict = {
        "CandidatePreferred": cfg.custom.create_dictionary_import_settings.umls_columns[
            0
        ],
        "CandidateCUI": cfg.custom.create_dictionary_import_settings.umls_columns[1],
        "Sources": cfg.custom.create_dictionary_import_settings.umls_columns[2],
    }
    df = df.rename(columns=rename_dict)

    return df
