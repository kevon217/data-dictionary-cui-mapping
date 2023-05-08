"""

Create subset of UMLS MTH and preprocess before creating embeddings for semantic search.

"""

import re

from prefect import flow
from pathlib import Path
import pandas as pd
from pandas.api.types import CategoricalDtype
from tqdm import tqdm

import data_dictionary_cui_mapping.utils.helper as helper
from data_dictionary_cui_mapping.utils.text_processing import clean_text

cfg = helper.compose_config.fn(
    config_path="../configs/semantic_search", config_name="embeddings", overrides=[]
)


@flow(flow_run_name="Creating UMLS MTH subset", log_prints=True)
def subset_umls(cfg):
    # GET UMLS MTH FOLDER
    if cfg.umls_subset.mth_local.dirpath_mth:
        print(
            "Using local MTH folder: {}".format(cfg.umls_subset.mth_local.dirpath_mth)
        )
    else:
        cfg.umls_subset.mth_local.dirpath_mth = helper.choose_dir(
            "Choose UMLS MTH folder"
        )
    dirpath_mth = Path(cfg.umls_subset.mth_local.dirpath_mth)

    # GET OUTPUT FOLDER
    if cfg.umls_subset.settings.dirpath_output:
        print(
            "Using local output folder: {}".format(
                cfg.umls_subset.settings.dirpath_output
            )
        )
    else:
        cfg.umls_subset.settings.dirpath_output = helper.choose_dir(
            "Choose output folder"
        )
    dirpath_output = Path(cfg.umls_subset.settings.dirpath_output)

    # CREATE DATAFRAMES FOR CONSO, DEF, STY, RANK
    print("Creating dataframes for CONSO, DEF, STY, RANK")
    df_CONSO = pd.read_csv(
        dirpath_mth / cfg.umls_subset.mth_local.RRF_files.concepts.filename,
        sep="|",
        header=None,
        names=cfg.umls_subset.mth_local.RRF_files.concepts.columns,
        low_memory=False,
    )
    df_DEF = pd.read_csv(
        dirpath_mth / cfg.umls_subset.mth_local.RRF_files.definitions.filename,
        sep="|",
        header=None,
        names=cfg.umls_subset.mth_local.RRF_files.definitions.columns,
        usecols=cfg.umls_subset.mth_local.RRF_files.definitions.subset,
    )
    df_STY = pd.read_csv(
        dirpath_mth / cfg.umls_subset.mth_local.RRF_files.semantic_types.filename,
        sep="|",
        header=None,
        names=cfg.umls_subset.mth_local.RRF_files.semantic_types.columns,
        usecols=cfg.umls_subset.mth_local.RRF_files.semantic_types.subset,
    )
    df_RANK = pd.read_csv(
        dirpath_mth / cfg.umls_subset.mth_local.RRF_files.termtype_rank.filename,
        sep="|",
        header=None,
        names=cfg.umls_subset.mth_local.RRF_files.termtype_rank.columns,
        usecols=cfg.umls_subset.mth_local.RRF_files.termtype_rank.subset,
        na_filter=False,
    )

    # TODO: keep track of removed rows and save to file

    # KEEP ONLY  ENG LAT
    print(f"Applying filters for {cfg.umls_subset.filters.items()}")
    for col, vals in cfg.umls_subset.filters.items():
        df_CONSO = df_CONSO[df_CONSO[col].isin(vals)]
    df_CONSO = df_CONSO[cfg.umls_subset.mth_local.RRF_files.concepts.subset]
    df_CONSO.to_pickle(
        Path(cfg.umls_subset.settings.dirpath_output, "df_CONSO_LAT=ENG.pkl")
    )  # TODO: make this a config option

    # REMOVE ROWS WITH NULL VALUE
    print("Removing rows with null values in DEF and CONSO")
    df_DEF = df_DEF[df_DEF["DEF"].notnull()]
    df_CONSO = df_CONSO[df_CONSO["STR"].notnull()]
    df_DEF["DEF_unprocessed"] = df_DEF[
        "DEF"
    ]  # save unprocessed definition for reference
    df_CONSO["STR_unprocessed"] = df_CONSO[
        "STR"
    ]  # save unprocessed string for reference

    ## TEXT CLEANING

    # REMOVE: **Definition:**, Definition:, Description:, WHAT:, ****
    print("Removing: **Definition:**, Definition:, Description:, WHAT:, ****")
    remove = r"(\*\*Definition:\*\*|Definition:|Description:|WHAT:|\*\*\*\*)"
    df_DEF["DEF"] = df_DEF["DEF"].str.replace(remove, "", regex=True)
    df_CONSO["STR"] = df_CONSO["STR"].str.replace(remove, "", regex=True)

    # REMOVE [PMID:*, etc.] #TODO: REMOVE [PMID:*, etc.]

    # CLEAN TEXT (html tags, urls, extra spaces) #TODO: make this more efficient
    tqdm.pandas(desc="Cleaning DEF column:")
    df_DEF["DEF"] = df_DEF["DEF"].progress_apply(
        lambda x: clean_text(x) if re.search(r"<[^<]+?>", x) else x
    )
    tqdm.pandas(desc="Cleaning STR column:")
    df_CONSO["STR"] = df_CONSO["STR"].progress_apply(
        lambda x: clean_text(x) if re.search(r"<[^<]+?>", x) else x
    )

    # RANK CUIS BY RANK FILE
    print("Sorting CUIs by precedence based on rank in TTY file")
    rank_sorter = CategoricalDtype(
        df_RANK["TTY"].unique(), ordered=True
    )  # sort by rank giving precedence to PN. some cuis don't have PN, so next in line will be chosen when duplicates dropped.
    df_CONSO = df_CONSO[df_CONSO["TTY"].notnull()]  # remove rows with null TTY
    df_CONSO["TTY"] = df_CONSO["TTY"].astype(rank_sorter)
    df_CONSO.sort_values("TTY", inplace=True)

    # RANK DEFINITION PRECEDENCE BY ORDER OF SAB ABUNDANCE
    print("Sorting definitions by precedence based on SAB abundance")
    sab_order = CategoricalDtype(
        df_DEF["SAB"].value_counts().sort_values(ascending=False).index, ordered=True
    )
    df_DEF["SAB"] = df_DEF["SAB"].astype(sab_order)
    df_DEF.sort_values("SAB", inplace=True)

    # MERGE SEMANTIC TYPES FOR EACH CUI
    print("Merging semantic types for each CUI")
    df_STY_flat = df_STY.groupby(["CUI"])["STY"].apply(";".join).reset_index()

    # MERGE SAB FOR EACH CUI
    print("Merging SAB for each CUI")
    conso_sab_concat = (
        df_CONSO[["CUI", "SAB"]]
        .groupby("CUI")["SAB"]
        .apply(lambda x: ";".join(set(x)))
        .reset_index()
    )
    def_sab_concat = (
        df_DEF[["CUI", "SAB"]]
        .groupby(["CUI"])["SAB"]
        .apply(lambda x: ";".join(set(x)))
        .reset_index()
    )

    # DROP DUPLICATES
    print("Dropping duplicates")
    df_CONSO.drop_duplicates(subset="CUI", inplace=True, keep="first")
    df_DEF.drop_duplicates(subset="CUI", inplace=True, keep="first")

    # MERGE DATAFRAMES
    print("Merging dataframes")
    df_CONSO = pd.merge(
        df_CONSO,
        conso_sab_concat,
        on="CUI",
        how="left",
        suffixes=(None, "_CUI_CONSO_all"),
    )
    df_DEF = pd.merge(
        df_DEF, def_sab_concat, on="CUI", how="left", suffixes=(None, "_CUI_DEF_all")
    )
    df_CONSO_STY = pd.merge(df_CONSO, df_STY_flat, on="CUI", how="left")
    df_umls = pd.merge(
        df_DEF, df_CONSO_STY, on="CUI", how="left"
    )  # dimension will be based on CUIs that have definitions
    df_umls.rename(columns={"SAB_x": "SAB_MRDEF", "SAB_y": "SAB_MRCONSO"}, inplace=True)

    # SAVE UMLS TEST SET0
    df_umls.to_pickle(Path(dirpath_output, "df_umls.pkl"))
    # str_check = df_umls[df_umls['STR'].str.contains(r'<[^<]+?>')]
    # def_check = df_umls[df_umls['DEF'].str.contains(r'<[^<]+?>')]

    # SAVE CONFIG
    helper.save_config(cfg, dirpath_output, "config_umls.yaml")

    return df_umls, cfg


if __name__ == "__main__":
    df_umls, cfg = subset_umls(cfg)
