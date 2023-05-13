"""

Functions to check the examples dictionary and its CUI mappings and report discrepancies.

"""
import pandas as pd
import numpy as np
import itertools


def get_check_columns(check_cuis: dict):
    """Returns list of columns to check"""
    check_columns = []
    # get all values from check columns key in nested dictionary
    for k1, v1 in check_cuis.items():
        for k2, v2 in v1.items():
            if k2 == "check_columns":
                check_columns.append(v2[:])
    check_columns = list(itertools.chain.from_iterable(check_columns))
    return check_columns


def count_sep(cell, sep):
    """Counts total number of separated values for a cell by separator specified"""

    if pd.isna(cell):
        return 0
    else:
        sep_split = cell.split(sep)
        n_sep = len(sep_split)
        return n_sep


def sep_list(cell, sep):
    cui_list = cell.split(sep)
    return cui_list


def idx_cui_map(cell, dict_map):
    if cell is np.nan:
        pass
    else:
        cell = cell.replace(dict_map[cell])


def count_sep_missing(cell, sep):
    "Counts number of missing (blank or empty) CUI codes and records their index"

    if pd.isna(cell):
        return 1
    else:
        vals = cell.split(sep)
        vals = list(
            map(lambda x: x.strip(), vals)
        )  # removes surrounding whitespace in case empty value has accidental space
        ls_is_missing = list(map(lambda x: x == 0, (map(len, vals))))
        idx_missing = [idx for idx, e in enumerate(ls_is_missing) if e is True]
        n_missing = sum(ls_is_missing)
        return n_missing


def idx_sep_missing(cell, sep):
    "Counts number of missing (blank or empty) CUI codes and records their index"

    if pd.isna(cell) or len(cell) == 0:
        return np.nan
    else:
        vals = cell.split(sep)
        vals = list(
            map(lambda x: x.strip(), vals)
        )  # removes surrounding whitespace in case empty value has accidental space
        ls_is_missing = list(map(lambda x: x == 0, (map(len, vals))))
        idx_missing = [idx for idx, e in enumerate(ls_is_missing) if e is True]
        n_missing = sum(ls_is_missing)
        return idx_missing


def count_cui_sep_missing(df_check, col, cui_sep):
    n_missing, idx_missing = zip(
        *[(x[0], x[1]) for x in list(map(count_sep_missing, df_check[col], cui_sep))]
    )
    return n_missing, idx_missing


def count_multi_cuis(cell, sep1, sep2):
    """Counts number of multicui entries"""
    if pd.isna(cell):
        return np.nan
    else:
        vals = cell.split(sep1)
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
        return n_mult


def idx_multi_cuis(cell, sep1, sep2):
    """Counts number of multicui entries"""
    if pd.isna(cell):
        return np.nan
    else:
        vals = cell.split(sep1)
        if sep2:
            ls_mult_cui = list(map(lambda x: x.count(sep2), vals))
            ls_mult_cui = [
                e + 1 if e > 0 else e for e in ls_mult_cui
            ]  # accounts for one less "/" than number of cuis
            idx_mult_cui = [idx for idx, e in enumerate(ls_mult_cui) if e > 0]
    return idx_mult_cui


def returnFlaggedCUIs(df_ref_ls, idx_flag):
    'Will spit out cuis based on the indices in a list of flags (e.g., missing, "Not Available", multiple cuis)'
    ls_flagged_cui = []
    for ls_vals, ls_idx_flag in zip(df_ref_ls, idx_flag):
        if type(ls_vals) is not list:  # case for when there are no values
            ls_flagged_cui.append("No values")
        elif (
            type(ls_idx_flag) is list or type(ls_idx_flag) is np.ndarray
        ):  # case for when there are PVDs
            if len(ls_idx_flag) == 0:  # case for when there are no missing
                ls_flagged_cui.append([])
            elif max(ls_idx_flag) >= len(ls_vals):
                ls_flagged_cui.append(ls_vals)
            else:
                ls_flagged_cui.append(
                    list(map(ls_vals.__getitem__, ls_idx_flag))
                )  # find PVD that is missing CUI by idx
        else:
            ls_flagged_cui.append("No values")
    return ls_flagged_cui
