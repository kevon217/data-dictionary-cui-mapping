"""

This file contains custom functions used in scripts designed to search, map and format UMLS CUIs for BRICS examples elements (DEs) and
permissible values/permissible value descriptions (PVs/PVDs).

"""

import re

from bs4 import BeautifulSoup
import html as ihtml
import cchardet
import pandas as pd
from prefect import task

from data_dictionary_cui_mapping.utils import helper

# TEXT PROCESSING FUNCTIONS


def check_query_terms_valid(query_term) -> bool:
    """Check if query term is valid"""

    if pd.isna(query_term) or len(query_term) == 0:
        valid = False
    else:
        valid = True
    return valid


def unescape_string(textstring):
    """Remove leading backslashes from text string"""

    return textstring.replace("\\", "")


@task(name="Removing punctuation")
def remove_punctuation(df, columns):
    """Remove punctuation"""

    df[columns] = df[columns].apply(
        lambda x: x.astype(str).str.replace(r"[^\w\s]", " ", regex=True)
    )
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


@task(name="Removing stopwords from query columns")
def remove_stopwords_cols(df, columns, preprocessing_settings):
    """Remove stopwords from list and keep track of modified terms and stopwords removed in dataframe columns"""

    cols_query_terms = []
    if preprocessing_settings.remove_stopwords:
        if preprocessing_settings.stopwords_filepath:
            print("Loading stopwords file from configs")
            fp_stopwords = preprocessing_settings.stopwords_filepath
        else:
            print("Opening dialog box to choose stopwords file")
            fp_stopwords = helper.choose_file("Select Stopwords csv file")
        df_stopwords = pd.read_csv(fp_stopwords)
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


def remove_vars_cheatsheet(df, preprocessing_settings):  # TODO: not yet implemented
    """Remove variables from examples dictionary that are already curated in a Cheatsheet csv file"""

    if preprocessing_settings.use_cheatsheet:
        if preprocessing_settings.cheatsheet_filepath:
            print("Loading cheatsheet file from configs")
            fp_cheatsheet = preprocessing_settings.cheatsheet_filepath
        else:
            print("Opening dialog box to choose cheatsheet file")
            fp_cheatsheet = helper.choose_file(title="Select Cheatsheet csv file")
        df_cheatsheet = pd.read_csv(fp_cheatsheet)
        curated_vars = df_cheatsheet[
            "variable name"
        ]  # TODO: need to add consistent formatting for use of a cheatsheet
        df = df[~df["variable name"].isin(curated_vars)]
    else:
        print("Cheatsheet not used")
        pass
    return df


# @task(name="Cleaning text")
def clean_text(text):
    text = BeautifulSoup(ihtml.unescape(text), "lxml").text
    text = re.sub(r"http[s]?://\S+", "", text)
    text = re.sub(r"\s+", " ", text)
    return text
