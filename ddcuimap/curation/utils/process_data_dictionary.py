"""

Pre/Post-processing Data Dictionary functions.

"""

import pandas as pd

from ddcuimap.curation import cur_logger, log, copy_log
from ddcuimap.utils import helper as helper
from ddcuimap.curation.utils import text_processing as tp


@log(msg="Loading data dictionary file")
def load_data_dictionary(cfg):
    """Load Data Dictionary file from filepath or choose with ui if not specified"""

    if not cfg.custom.data_dictionary_settings.filepath:
        fp_dd = helper.choose_file("Select data dictionary csv input file")
        df_dd = pd.read_csv(fp_dd)
        cur_logger.info(f"Data Dictionary shape is: {df_dd.shape}")
        cfg.custom.data_dictionary_settings.filepath = fp_dd
    else:
        fp_dd = cfg.custom.data_dictionary_settings.filepath
        cur_logger.warning(f"Loading data dictionary from filepath in configs.")
        df_dd = pd.read_csv(fp_dd)
        cur_logger.info(f"Data Dictionary shape is: {df_dd.shape}")
    return df_dd, fp_dd


@log(msg="Exploding values in columns to query with")
def explode_dictionary(df, explode, query_term_columns, column_sep):
    """Explode dictionary column/s separated values"""

    if explode:
        cols_extracted = []
        for col in query_term_columns:
            col_extracted = f"{col}_extracted"
            df[col_extracted] = (
                df[col].str.split(column_sep).fillna("")
            )  # turns separated values into list for explode step
            cols_extracted.append(col_extracted)
        df = df.explode(cols_extracted, ignore_index=True)  # explode PVs/PVDs
    else:
        # cols_extracted = [] # TODO: figure out if this is needed
        for col in query_term_columns:
            col_extracted = f"{col}_extracted"
            df[col_extracted] = df[col]
            # cols_extracted.append(col_extracted) # TODO: figure out if this is needed
    return df


@log(msg="Preprocessing data dictionary")
def process_data_dictionary(df_dd, cfg):
    """Main preprocessing pipeline for data dictionary"""

    cols_extracted = []
    for col_name in cfg.custom.data_dictionary_settings.query_term_columns:
        cols_extracted.append(f"{col_name}_extracted")
    df_dd_preprocessed = (
        df_dd.copy()
        .pipe(
            tp.remove_vars_cheatsheet, cfg.custom.preprocessing_settings
        )  # TODO: will implement in future
        .pipe(
            explode_dictionary,
            cfg.custom.data_dictionary_settings.explode,
            cfg.custom.data_dictionary_settings.query_term_columns,
            cfg.custom.data_dictionary_settings.column_sep,
        )
        .pipe(tp.remove_punctuation, cols_extracted)
        .pipe(
            tp.remove_stopwords_cols, cols_extracted, cfg.custom.preprocessing_settings
        )
    )
    cur_logger.info(f"Processed Data Dictionary shape is: {df_dd_preprocessed.shape}")
    return df_dd_preprocessed
