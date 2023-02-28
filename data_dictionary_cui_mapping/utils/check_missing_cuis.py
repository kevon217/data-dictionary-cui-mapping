# CHECK FOR MISSING CUIS IN DEs and/or PVs

"""
This scripts checks final modified data import template for missing data element and/or permissible value cuis.

It can be set to run within create_dictionary_import_file.py and DE_UMLS_FITBIR_DE_REPORT_merge or standalone.
"""

# LOAD PACKAGES

import os
import tkinter as tk  # for dialog boxes
from functools import partial
from tkinter import filedialog

import numpy as np
import pandas as pd

root = tk.Tk()
root.withdraw()

# VERIFICATION FUNCTIONS


def count_sep(cell, sep):
    "Counts total number of separated values for a data element cell by separator specified"
    if pd.isna(cell) or len(cell) == 0:
        return (np.nan, np.nan)
    else:
        sep_split = cell.split(sep)  # PVDs separated by ";"
        n_sep = int(len(sep_split))
        return n_sep, sep_split


def count_PV_CUIs_missing(cell):
    "Counts number of missing (blank or empty) CUI codes and records their index"
    if pd.isna(cell):
        return (np.nan, np.nan)
    else:
        vals = cell.split("|")  # CUIs separated by "|"
        vals = list(
            map(lambda x: x.strip(), vals)
        )  # removes surrounding whitespace in case empty value has accidental space
        ls_is_missing = list(map(lambda x: x == 0, (map(len, vals))))
        idx_missing = [idx for idx, e in enumerate(ls_is_missing) if e is True]
        n_missing = sum(ls_is_missing)
        return n_missing, idx_missing


def count_DE_CUIs_missing(cell):
    "Counts number of missing (blank or empty) CUI codes and records their index"
    if pd.isna(cell):
        return (np.nan, np.nan)
    else:
        vals = cell.split("/")  # multiple CUIs separated by "/" for single data element
        vals = list(
            map(lambda x: x.strip(), vals)
        )  # removes surrounding whitespace in case empty value has accidental space
        ls_is_missing = list(map(lambda x: x == 0, (map(len, vals))))
        idx_missing = [idx for idx, e in enumerate(ls_is_missing) if e is True]
        n_missing = sum(ls_is_missing)
        return n_missing, idx_missing


def count_CUIs_NA(cell):
    'Counts number of CUIs marked as "Not Available"'
    if pd.isna(cell):
        return (np.nan, np.nan)
    else:
        vals = cell.split("|")  # multiple CUIs for a PVD separated by "/"
        ls_is_NA = list(map(lambda x: x == "Not Available", vals))
        idx_NA = [idx for idx, e in enumerate(ls_is_NA) if e is True]
        n_NA = sum(ls_is_NA)
        return n_NA, idx_NA


def count_CUIs_per_sep(cell, sep1, sep2):
    "Counts number of CUIs associated with each separated item"
    if pd.isna(cell):
        return (np.nan, np.nan)
    else:
        vals = cell.split(sep1)  # multiple CUIs for a PVD separated by "/"
        if sep2:
            ls_mult_cui = list(map(lambda x: x.count(sep2), vals))
            ls_mult_cui = [
                e + 1 if e > 0 else e for e in ls_mult_cui
            ]  # accounts for one less "/" than number of cuis
            idx_mult_cui = [idx for idx, e in enumerate(ls_mult_cui) if e > 0]
            n_mult = sum(e > 0 for e in ls_mult_cui)
        else:
            if len(vals) > 1:
                ls_mult_cui = list(map((lambda x: 1 if (len(x) > 0) else 0), vals))
                idx_mult_cui = [idx for idx, e in enumerate(ls_mult_cui)]
                n_mult = sum(e > 0 for e in ls_mult_cui)
            else:
                idx_mult_cui = []
                n_mult = 0

        return n_mult, idx_mult_cui


def returnFlaggedCUIs(cell, idx_flag):
    'Will spit out cuis based on the indices in a list of flags (e.g., missing, "Not Available", multiple cuis)'
    ls_flagged_cui = []
    for ls_vals, ls_idx_flag in zip(cell, idx_flag):
        if type(ls_vals) is not list:  # case for when there are no PVDs
            ls_flagged_cui.append(ls_vals)
        elif type(ls_idx_flag) is list:  # case for when there are PVDs
            if len(ls_idx_flag) == 0:  # case for when there are no missing PVDs
                ls_flagged_cui.append([])
            elif max(ls_idx_flag) > len(ls_vals):
                ls_flagged_cui.append("Too many CUIs to map.")
            else:
                ls_flagged_cui.append(
                    list(map(ls_vals.__getitem__, ls_idx_flag))
                )  # find PVD that is missing CUI by idx
        else:
            print(f"Not a list of indexes: {ls_idx_flag}")
    return ls_flagged_cui


def create_folder(folder_path):  # creates new folders every time script is run
    adjusted_folder_path = folder_path
    folder_found = os.path.isdir(adjusted_folder_path)
    counter = 0
    while folder_found == True:
        counter = counter + 1
        adjusted_folder_path = folder_path + " (" + str(counter) + ")"
        folder_found = os.path.isdir(adjusted_folder_path)
    os.mkdir(adjusted_folder_path)
    return adjusted_folder_path


# RUN AS STANDALONE

if __name__ == "__main__":
    ## choose data element import template to verify
    fn_df_final = filedialog.askopenfilename(
        title="Choose '{}_step2_XYZ_dataElementImport_cuis.csv' file"
    )
    df_final = pd.read_csv(fn_df_final)  # , keep_default_na = False)
    filename = os.path.basename(fn_df_final).split("_")[1]
    dir_verify = create_folder(
        os.path.dirname(os.path.dirname(fn_df_final))
        + "/VERIFY_name-main_Check Missing CUIs"
    )

    ## check if new umls columns for DE or PV exist
    cols_de = [
        "data element concept names",
        "data element concept identifiers",
        "data element terminology sources",
    ]
    cols_pv = [
        "permissible value concept names",
        "permissible value concept identifiers",
        "permissible value terminology sources",
    ]
    cols_present = [col for col in df_final.columns if col in [*cols_de, *cols_pv]]
    df_verify = df_final[
        [
            "variable name",
            "title",
            "definition",
            "permissible values",
            "permissible value descriptions",
            "permissible value output codes",
            *cols_present,
        ]
    ].copy()
    if all(col in cols_present for col in cols_de):
        de_verify = 1
    else:
        de_verify = 0
    if all(col in cols_present for col in cols_pv):
        pv_verify = 1
    else:
        pv_verify = 0

    ## de verification

    if de_verify:
        ## select relevant columns
        df_verify = df_final[
            [
                "variable name",
                "title",
                "definition",
                "permissible values",
                "permissible value descriptions",
                "permissible value output codes",
                "data element concept names",
                "data element concept identifiers",
                "data element terminology sources",
            ]
        ].copy()

        ## de verification
        n_des, des = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        partial(count_sep, sep="|"),
                        df_verify["data element concept names"],
                    )
                )
            ]
        )
        n_missing, idx_missing = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        count_DE_CUIs_missing,
                        df_verify["data element concept identifiers"],
                    )
                )
            ]
        )
        n_NA, idx_NA = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(count_CUIs_NA, df_verify["data element concept identifiers"])
                )
            ]
        )
        n_mult, idx_mult_cui = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        partial(count_CUIs_per_sep, sep1="|", sep2="/"),
                        df_verify["data element concept identifiers"],
                    )
                )
            ]
        )

        ls_de_missing_cui = returnFlaggedCUIs(des, idx_missing)
        ls_de_cui_NA = returnFlaggedCUIs(des, idx_NA)
        ls_de_mult_cui = returnFlaggedCUIs(des, idx_mult_cui)

        n_de_missing_cui = list(
            map(lambda x: len(x) if type(x) is list else 1, ls_de_missing_cui)
        )
        n_de_cui_NA = list(
            map(lambda x: len(x) if type(x) is list else np.nan, ls_de_cui_NA)
        )
        # n_de_mult_cui = list(map(lambda x: len(x), ls_de_mult_cui))
        # n_de_mult_cui = list(map(lambda x: (len(x) if len(x) > 0 else 1) if type(x) is list else 0, ls_de_mult_cui))
        # n_single_mult_cui = np.array(n_des) - np.array(n_mult)

        ## add verification output as columns
        df_verify["de missing cui"] = ls_de_missing_cui
        df_verify["total # de missing cui"] = n_de_missing_cui
        df_verify["de cui Not Available"] = ls_de_cui_NA
        df_verify["total # de cuis Not Available"] = n_de_cui_NA
        df_verify["total # of cui terms"] = list(n_des)
        df_verify["total # multi-cui terms"] = list(n_mult)
        df_verify["de with multiple cuis"] = ls_de_mult_cui

    ## pv verification

    if pv_verify:
        n_pvds, pvds = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        partial(count_sep, sep="|"),
                        df_verify["permissible value descriptions"],
                    )
                )
            ]
        )
        n_missing, idx_missing = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        count_PV_CUIs_missing,
                        df_verify["permissible value concept identifiers"],
                    )
                )
            ]
        )
        n_NA, idx_NA = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        count_CUIs_NA,
                        df_verify["permissible value concept identifiers"],
                    )
                )
            ]
        )
        n_mult, idx_mult_cui = zip(
            *[
                (x[0], x[1])
                for x in list(
                    map(
                        partial(count_CUIs_per_sep, sep1="|", sep2="/"),
                        df_verify["permissible value concept identifiers"],
                    )
                )
            ]
        )

        ls_pvd_missing_cui = returnFlaggedCUIs(pvds, idx_missing)
        ls_pvd_cui_NA = returnFlaggedCUIs(pvds, idx_NA)
        ls_pvd_mult_cui = returnFlaggedCUIs(pvds, idx_mult_cui)

        n_pvd_missing_cui = list(
            map(lambda x: len(x) if type(x) is list else np.nan, ls_pvd_missing_cui)
        )
        n_pvd_cui_NA = list(
            map(lambda x: len(x) if type(x) is list else np.nan, ls_pvd_cui_NA)
        )
        n_pvd_mult_cui = list(
            map(lambda x: len(x) if type(x) is list else np.nan, ls_pvd_mult_cui)
        )

        ## add verification output as columns
        df_verify["pvds missing cui"] = ls_pvd_missing_cui
        df_verify["total # pvds missing cui"] = n_pvd_missing_cui
        df_verify["pvds cuis Not Available"] = ls_pvd_cui_NA
        df_verify["total # cuis Not Available"] = n_pvd_cui_NA
        df_verify["pvds with multiple cuis"] = ls_pvd_mult_cui
        df_verify["total # of pvds with multiple cuis"] = n_pvd_mult_cui

    ## write verification file

    fp_verify = f"{dir_verify}/VERIFY_{filename}_DE-{de_verify}_PV-{pv_verify}_check-missing_cui.csv"
    df_verify.to_csv(fp_verify, index=False)
