# data-dictionary-cui-mapping

This package allows you to  load in a data dictionary and map cuis to defined fields using either the UMLS API or MetaMap API from NLM.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install data-dictionary-cui-mapping
```

## Usage

```python
# import batch_query_pipeline moduel from metamap package
from data_dictionary_cui_mapping.metamap import batch_query_pipeline

# import helper functions for loading, viewing, composing configurations for pipeline run
from data_dictionary_cui_mapping.utils import helper
from omegaconf import OmegaConf

# import modules to create data dictionary with curated CUIs and check the file for missing mappings
from data_dictionary_cui_mapping.curation import create_dictionary_import_file
from data_dictionary_cui_mapping.curation import check_cuis

# LOAD/EDIT CONFIGURATION FILES
cfg = helper.compose_config.fn(overrides=["custom=de", "apis=config_metamap_api"])
cfg.apis.user_info.email = ''
cfg.apis.user_info.apiKey = ''
print(OmegaConf.to_yaml(cfg))

# STEP-1: RUN BATCH QUERY PIPELINE
df_final = batch_query_pipeline.main(cfg)

# MANUAL CURATION STEP IN EXCEL FILE

# STEP-2: CREATE DATA DICTIONARY IMPORT FILE
cfg = helper.load_config.fn(helper.choose_input_file.fn("Load config file from Step 1"))
create_dictionary_import_file.main(cfg)

# CHECK CURATED CUI MAPPINGS
cfg = helper.load_config.fn(helper.choose_input_file.fn("Load config file from Step 2"))
check_cuis.main(cfg)


```

## Acknowledgements

The MetaMap API code included is from Will J Roger's repository --> https://github.com/lhncbc/skr_web_python_api

Special thanks to Olga Vovk and Henry Ogoe for their guidance, feedback, and testing of this package.

## License

[MIT](https://choosealicense.com/licenses/mit/)
