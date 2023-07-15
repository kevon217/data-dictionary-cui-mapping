import pandas as pd
import ddcuimap.curation.utils.process_data_dictionary as proc_dd
import ddcuimap.utils.helper as helper
import re


de_cols = ["data element concept identifiers", "data element concept names"]
pv_cols = ["permissible value concept identifiers", "permissible value concept names"]


# LOAD DATA DICTIONARY CSV

dd_fp = helper.choose_file("Select data dictionary csv input file")
dd = pd.read_csv(dd_fp, dtype="object")

# FILTER ON DATA ELEMENTS WITH ASSIGNED CONCEPT IDS
dd_de = dd[dd["data element concept identifiers"].notna()].reset_index(drop=True)
dd_pv = dd[dd["permissible value concept identifiers"].notna()].reset_index(drop=True)

# write regular expression to split strings by multiple delimiters with re

pattern = re.compile(r"[|/]")


def split_col(df, col, sep):
    df[col] = df[col].str.split(sep)
    return df


# split_col to each column in cols_to_explode
for col in de_cols:
    split_col(dd_de, col, pattern)

dd_de_exp = dd_de.explode(de_cols).reset_index(drop=True)

id = dd_de["data element concept identifiers"].apply(len)
name = dd_de["data element concept names"].apply(len)

# find where id and name are not equal and get idx

id_ne_name = id != name
idx = id_ne_name[id_ne_name].index
test = dd_de.loc[idx]

test.to_csv("test.csv")
