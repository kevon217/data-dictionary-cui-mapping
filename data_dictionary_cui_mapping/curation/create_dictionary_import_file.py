"""

Main script for creating dictionary import file from curated examples dictionary --> UMLS CUI mappings excel file.

"""

from prefect import flow
from pathlib import Path
from data_dictionary_cui_mapping.utils import helper as helper
from data_dictionary_cui_mapping.curation.utils import curation_functions as cur


# @hydra.main(version_base=None, config_path="../configs", config_name="config")
@flow(flow_run_name="Creating dictionary import file", log_prints=True)
def create_dd_file(cfg):
    # LOAD "*_Step-1_curation_keepCol.xlsx" file
    if not cfg.custom.create_dictionary_import_settings.curation_file_path:
        fp_curation = cur.get_curation_excel_file(
            "Select *_Step-1_curation_keepCol.xlsx file with curated CUIs"
        )
        cfg.custom.create_dictionary_import_settings.curation_file_path = fp_curation
    else:
        fp_curation = cfg.custom.create_dictionary_import_settings.curation_file_path
    (
        df_UMLS_curation,
        df_Data_Dictionary,
        df_Data_Dictionary_extracted,
    ) = cur.load_curation_excel_file.fn(
        fp_curation, cfg
    )  # load curation Excel file

    dir_step2 = helper.create_folder.fn(
        Path(fp_curation).parent.joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_Step-2_create-dictionary-import-file"
        )
    )

    # POSTPROCESSING PIPELINE
    cols_join_on = list(cfg.custom.create_dictionary_import_settings.join_on)
    umls_columns = list(cfg.custom.create_dictionary_import_settings.umls_columns)
    df_final = (
        df_UMLS_curation.copy()
        .pipe(cur.filter_keep_col)
        .pipe(cur.order_keep_col)
        .pipe(cur.concat_mult_cuis, cols_join_on, umls_columns)
        .pipe(
            cur.merge_with_dictionary,
            df_right=df_Data_Dictionary_extracted,
            how="right",
            cols_join_on=cols_join_on,
            suffixes=("", "_y"),
        )
        .fillna("")
        .pipe(
            cur.concat_cols_umls,
            cfg.custom.create_dictionary_import_settings.umls_columns,
        )
        .pipe(
            cur.merge_with_dictionary,
            df_right=df_Data_Dictionary,
            how="right",
            cols_join_on=cols_join_on[0],
            suffixes=("", "_y"),
        )
        .pipe(
            cur.reorder_cols,
            cfg.custom.create_dictionary_import_settings.dictionary_columns,
        )
        .pipe(cur.override_cols, cfg.custom.create_dictionary_import_settings.override)
    )

    # SAVE FINALIZED IMPORT TEMPLATE
    fp_step2 = f"{dir_step2}/{cfg.custom.curation_settings.file_settings.file_prefix}_Step-2_dictionary-import-file.csv"
    cfg.custom.create_dictionary_import_settings.dict_file_path = fp_step2
    df_final.to_csv(fp_step2, index=False)  # output df_final dataframe to csv
    print(f"Saved {fp_step2}")
    helper.save_config(cfg, dir_step2)

    return df_final


if __name__ == "__main__":
    cfg = helper.load_config.fn(helper.choose_file("Load config file from Step 1"))
    df_final = create_dd_file(cfg)
