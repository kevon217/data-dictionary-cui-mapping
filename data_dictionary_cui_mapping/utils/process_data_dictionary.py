import pandas as pd
from prefect import flow, task

from . import helper as helper
from . import text_processing as tp


@flow(flow_run_name="Loading data dictionary file")
def load_data_dictionary(cfg):
    """Load Data Dictionary file from filepath or choose with ui if not specified"""

    if not cfg.custom.data_dictionary_settings.filepath:
        fp_dd = helper.choose_input_file("Select data dictionary csv input file")
        df_dd = pd.read_csv(fp_dd)
        print(f"Data Dictionary shape is: {df_dd.shape}")
        cfg.custom.data_dictionary_settings.filepath = fp_dd
    else:
        fp_dd = cfg.custom.data_dictionary_settings.filepath
        print(f"Loading data dictionary from filepath in configs.")
        df_dd = pd.read_csv(fp_dd)
        print(f"Data Dictionary shape is: {df_dd.shape}")
    return df_dd, fp_dd


@task(name="Exploding values in columns to query with")
def explode_dictionary(df, query_term_columns, column_sep):
    """Explode dictionary column/s separated values"""

    cols_exploded = []
    for col in query_term_columns:
        col_exploded = f"{col}_exploded"
        df[col_exploded] = (
            df[col].str.split(column_sep).fillna("")
        )  # turns separated values into list for explode step
        cols_exploded.append(col_exploded)
    df = df.explode(cols_exploded, ignore_index=True)  # explode PVs/PVDs
    return df


@flow(flow_run_name="Processing data dictionary")
def process_data_dictionary(df_dd, cfg):
    """Main preprocessing pipeline for data dictionary"""

    cols_exploded = []
    for colname in cfg.custom.data_dictionary_settings.query_term_columns:
        cols_exploded.append(f"{colname}_exploded")
    df_dd_preprocessed = (
        df_dd.copy()
        .pipe(
            tp.remove_vars_cheatsheet, cfg.custom.preprocessing_settings
        )  # TODO: will implement in future
        .pipe(
            explode_dictionary,
            cfg.custom.data_dictionary_settings.query_term_columns,
            cfg.custom.data_dictionary_settings.column_sep,
        )
        .pipe(tp.remove_punctuation, cols_exploded)
        .pipe(
            tp.remove_stopwords_cols, cols_exploded, cfg.custom.preprocessing_settings
        )
    )
    print(f"Processed Data Dictionary shape is: {df_dd_preprocessed.shape}")
    return df_dd_preprocessed
