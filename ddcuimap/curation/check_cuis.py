"""

Main script for checking CUIS in created dictionary import file.

"""
import os
import pandas as pd
import numpy as np

from ddcuimap.utils import helper as helper
from ddcuimap.curation import cur_logger, log, copy_log
from ddcuimap.curation.utils import dictionary_functions as dd


@log(msg="Checking CUIS in Data Dictionary Import File")
def check_cuis(cfg):
    # LOAD "*_Step-2_dictionary-import-file.csv" file
    if not cfg.custom.create_dictionary_import_settings.dict_file_path:
        fp_dict = helper.choose_file(
            "Select *_Step-2_dictionary-import-file.csv file with created dictionary import file"
        )
        cfg.custom.create_dictionary_import_settings.filepath = fp_dict
    else:
        fp_dict = cfg.custom.create_dictionary_import_settings.dict_file_path
    df_dict = pd.read_csv(fp_dict)
    dir_check = os.path.dirname(fp_dict)

    information_columns = list(cfg.custom.curation_settings.information_columns)
    check_columns = dd.get_check_columns(
        cfg.custom.create_dictionary_import_settings.check_cuis
    )
    df_check = df_dict[[*information_columns, *check_columns]].copy()

    for check in cfg.custom.create_dictionary_import_settings.check_cuis:
        ref_col = cfg.custom.create_dictionary_import_settings.check_cuis[check][
            "reference_column"
        ]
        df_ref_ls = df_check[ref_col].apply(
            lambda x: dd.sep_list(
                x, cfg.custom.create_dictionary_import_settings.cui_sep
            )
            if not pd.isna(x)
            else x
        )
        check_cols = cfg.custom.create_dictionary_import_settings.check_cuis[check][
            "check_columns"
        ]
        df_temp = df_check[check_cols].copy()

        # number of cuis
        df_n_cuis = df_temp.apply(
            np.vectorize(dd.count_sep),
            sep=cfg.custom.create_dictionary_import_settings.cui_sep,
        )  # Check CUIs
        df_n_cuis = df_n_cuis.add_suffix("_n_cuis")
        df_check = df_check.join(df_n_cuis, how="outer")

        # number of missing cuis
        df_n_missing = df_temp.apply(
            np.vectorize(dd.count_sep_missing),
            sep=cfg.custom.create_dictionary_import_settings.cui_sep,
        )  # Check CUIs
        df_n_missing = df_n_missing.add_suffix("_n_missing")
        df_check = df_check.join(df_n_missing, how="outer")
        df_cui_missing = df_temp.applymap(
            dd.idx_sep_missing,
            sep=cfg.custom.create_dictionary_import_settings.cui_sep,
        )
        for col in df_cui_missing.items():
            col_idx_missing = col[1]
            ls_missing_cui = dd.return_flagged_cuis(df_ref_ls, col_idx_missing)
            df_cui_missing[col[0]] = ls_missing_cui
        df_cui_missing = df_cui_missing.add_suffix("_cui_missing ")
        df_check = df_check.join(df_cui_missing, how="outer")

        # number of multi cuis
        df_n_multi_cui = df_temp.applymap(
            np.vectorize(dd.count_multi_cuis),
            sep1=cfg.custom.create_dictionary_import_settings.cui_sep,
            sep2=cfg.custom.create_dictionary_import_settings.multi_cui_sep,
        )  # Check CUIs
        df_n_multi_cui = df_n_multi_cui.add_suffix("_n_multi_cui")
        df_check = df_check.join(df_n_multi_cui, how="outer")

        df_multi_cui = df_temp.applymap(
            np.vectorize(dd.idx_multi_cuis),
            sep1=cfg.custom.create_dictionary_import_settings.cui_sep,
            sep2=cfg.custom.create_dictionary_import_settings.multi_cui_sep,
        )  # Check CUIs
        for col in df_multi_cui.items():
            col_multi_cui = col[1]
            ls_multi_cui = dd.return_flagged_cuis(df_ref_ls, col_multi_cui)
            df_multi_cui[col[0]] = ls_multi_cui
        df_multi_cui = df_multi_cui.add_suffix("_multi_cui")
        df_check = df_check.join(df_multi_cui, how="outer")

        cur_logger.info("Done checking CUIs for " + check)

    # SAVE FILE AND MOVE LOG
    fp_check = os.path.join(dir_check, "dictionary-import-file-check.csv")
    df_check.to_csv(fp_check, index=False)
    cur_logger.info("Saved file to " + fp_check)
    copy_log(cur_logger, dir_check, "dictionary-import-file-check.log")

    return df_check


if __name__ == "__main__":
    cfg = helper.load_config(helper.choose_file("Load config file from Step 2"))
    df_check = check_cuis(cfg)
