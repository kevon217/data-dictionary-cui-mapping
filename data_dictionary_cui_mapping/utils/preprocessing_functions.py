"""

This file contains custom functions used in scripts designed to search, map and format UMLS CUIs for BRICS data elements (DEs) and
permissible values/permissible value descriptions (PVs/PVDs).

"""

import pandas as pd
from data_dictionary_cui_mapping.utils import ui_functions as ui

# TEXT PROCESSING FUNCTIONS


def unescape_string(textstring):
    """Remove leading backslashes from text string"""
    return textstring.replace("\\", "")


def remove_punctuation(df, columns):
    """Remove punctuation"""

    df[columns] = df[columns].apply(
        lambda x: x.astype(str).str.replace(r"[^\w\s]", " ")
    )  # remove punctuation
    return df


def remove_stopwords_text(text, ls_stopwords):
    """Remove stopwords from list and keep track of removed in separate list"""
    ls_removed = []
    output = list(
        filter(
            None,
            [
                word.lower()
                if word.lower() not in ls_stopwords
                else ls_removed.append(word)
                for word in text.split()
            ],
        )
    )
    output = " ".join(output)
    ls_removed = ";".join(ls_removed)
    return output, ls_removed


def remove_stopwords_cols(df, columns, rmv_stopwords):
    """Remove stopwords from list and keep track of removed in separate list"""

    cols_query_terms = []
    if rmv_stopwords:
        fp_stopwords = ui.choose_input_file(
            "Select MetaMap_Settings_StopWords.csv file"
        )
        df_stopwords = pd.read_csv(fp_stopwords)
        # will remove in future version of stopwords file
        # df_stopwords = df_stopwords[df_stopwords['StopWord?'] == 'Yes']
        ls_stopwords = list(
            df_stopwords["Word"].str.lower().str.strip()
        )  # make lower case and remove leading/trailing whitespaces
        for e, col in enumerate(columns):
            col_query_term = f"query_term_{e + 1}"
            cols_query_terms.append(col_query_term)
            col_query_term_stopwords_removed = f"query_term_stopwords_removed_{e + 1}"
            df[[col_query_term, col_query_term_stopwords_removed]] = (
                df[col].apply(remove_stopwords_text, args=(ls_stopwords,)).to_list()
            )
    else:
        for e, col in enumerate(columns):
            col_query_term = f"query_term_{e + 1}"
            cols_query_terms.append(col_query_term)
            col_query_term_stopwords_removed = f"query_term_stopwords_removed_{e + 1}"
            df[col_query_term] = df[col]
            df[col_query_term_stopwords_removed] = ""
    return df


def remove_vars_cheatsheet(df, use_cheatsheet):  # TODO: not ready
    """Remove variables from data dictionary that are already curated in the MetaMap_Settings_Cheatsheet.csv file"""
    if use_cheatsheet:
        fp_cheatsheet = ui.choose_input_file(
            title="Select MetaMap_Settings_Cheatsheet.csv file"
        )
        df_cheatsheet = pd.read_csv(fp_cheatsheet)
        curated_vars = df_cheatsheet[
            "variable name"
        ]  # TODO: need to add consistent formatting for use of a cheatsheet
        df = df[~df["variable name"].isin(curated_vars)]
    else:
        pass
    return df


# MAIN SCRIPT FUNCTIONS


def explode_dictionary(df, cols_pv, sep):
    """Explode dictionary column/s separated values"""

    cols_exploded = []
    for col in cols_pv:
        col_exploded = f"{col}_exploded"
        df[col_exploded] = (
            df[col].str.split(sep).fillna("")
        )  # turns separated values into list for explode step
        cols_exploded.append(col_exploded)
    df = df.explode(cols_exploded, ignore_index=True)  # explode PVs/PVDs
    return df


def preprocess_data_dictionary(df_dd: pd.DataFrame, cfg: dict):
    """Main preprocessing pipeline for data dictionary"""

    cols_exploded = []
    for colname in cfg.custom.query_settings.query_term_columns:
        cols_exploded.append(f"{colname}_exploded")
    df_dd_preprocessed = (
        df_dd.copy()
        .pipe(remove_vars_cheatsheet, cfg.custom.preprocessing_settings.use_cheatsheet)
        .pipe(  # use cheatsheet # TODO: need to implement this in the future
            explode_dictionary,
            cfg.custom.query_settings.query_term_columns,
            cfg.custom.input_file_settings.input_sep,
        )
        .pipe(remove_punctuation, cols_exploded)  # explode PVs
        .pipe(  # remove punctuation
            remove_stopwords_cols,
            cols_exploded,
            cfg.custom.preprocessing_settings.remove_stopwords,
        )
    )  # remove stopwords
    return df_dd_preprocessed
