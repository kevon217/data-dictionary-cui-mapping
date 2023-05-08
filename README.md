# data-dictionary-cui-mapping

This package allows you to load in a data dictionary and semi-automatically query appropriate UMLS concepts using either the UMLS API, MetaMap API, and/or Semantic Search through a custom Pinecone vector database .

## Prerequisites

- For UMLS API and MetaMap API, you will need to have an account with the UMLS API and/or MetaMap API. You can sign up for an account here: https://www.nlm.nih.gov/research/umls/index.html
- For Semantic Search with Pinecone, you will need to have an account with Pinecone. You can sign up for an account here: https://www.pinecone.io/. Please reach out to me if you would like temporary access to my Pinecone index to explore these embeddings.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install data-dictionary-cui-mapping or pip install from the GitHub repo.

```bash
pip install data-dictionary-cui-mapping
#pip install git+https://github.com/kevon217/data-dictionary-cui-mapping.git
```

## Input: Data Dictionary

Below is a sample data dictionary format that can be used as input for this package.

| variable name | title                  | permissible value descriptions |
| ------------- | ---------------------- |--------------------------------|
| AgeYrs        | Age in years           |                                |
| CaseContrlInd | Case control indicator | Case;Control;Unknown           |

## Configuration Files

In order to run and customize these pipelines, you will need to create/edit yaml configuration files located in configs. Run configurations are saved and can be reloaded.

```bash
├───data_dictionary_cui_mapping
│   ├───configs
│   │   │   config.yaml
│   │   │   __init__.py
│   │   │
│   │   ├───apis
│   │   │       __init__.py
│   │   │       config_metamap_api.yaml
│   │   │       config_pinecone_api.yaml
│   │   │       config_umls_api.yaml
│   │   │
│   │   ├───custom
│   │   │       de.yaml
│   │   │       hydra_base.yaml
│   │   │       pvd.yaml
│   │   │       title_def.yaml
│   │   │
│   │   ├───semantic_search
│   │   │       embeddings.yaml
```

## UMLS API and MetaMap Batch Queries

#### Import modules
```python
# import batch_query_pipeline modules from metamap OR umls package
from data_dictionary_cui_mapping.metamap import batch_query_pipeline as mm_bqp
from data_dictionary_cui_mapping.umls import batch_query_pipeline as umls_bqp

# import helper functions for loading, viewing, composing configurations for pipeline run
from data_dictionary_cui_mapping.utils import helper
from omegaconf import OmegaConf

# import modules to create data dictionary with curated CUIs and check the file for missing mappings
from data_dictionary_cui_mapping.curation import create_dictionary_import_file
from data_dictionary_cui_mapping.curation import check_cuis
```
#### Load/edit configuration files
```python
cfg = helper.compose_config.fn(overrides=["custom=de", "apis=config_metamap_api"]) # custom config for MetaMap on data element 'title' column
# cfg = helper.compose_config.fn(overrides=["custom=de", "apis=config_umls_api"]) # custom config for UMLS API on data element 'title' column
# cfg = helper.compose_config.fn(overrides=["custom=pvd", "apis=config_metamap_api"]) # custom config for MetaMap on 'permissible value descriptions' column
# cfg = helper.compose_config.fn(overrides=["custom=pvd", "apis=config_umls_api"]) # custom config for UMLS API on 'permissible value descriptions' column
cfg.apis.user_info.email = '' # enter your email
cfg.apis.user_info.apiKey = '' # enter your api key
print(OmegaConf.to_yaml(cfg))
```

#### Step 1: Run batch query pipeline
```python
df_final_mm = mm_bqp.run_mm_batch(cfg) # run MetaMap batch query pipeline
# df_final_umls = umls_bqp.run_umls_batch(cfg) # run UMLS API batch query pipeline
```

#### Step 2: *Manual curation step in excel file

*see curation example in ***notebooks/examples_files/DE_Step-1_curation_keepCol.xlsx***

#### Step 3: Create data dictionary import file

```python
cfg = helper.load_config.fn(helper.choose_file.fn("Load config file from Step 1"))
create_dictionary_import_file.create_dd_file(cfg)
```

#### Step 4: Check curated cui mappings

```python
cfg = helper.load_config.fn(helper.choose_file.fn("Load config file from Step 2"))
check_cuis.check_cuis(cfg)
```

## Output: Data Dictionary + CUIs
Below is the final output of the data dictionary with curated CUIs.

| variable name | title                  | data element concept identifiers | data element concept names | data element terminology sources | permissible values   | permissible value descriptions | permissible value output codes | permissible value concept identifiers | permissible value concept names           | permissible value terminology sources |
| ------------- | ---------------------- | -------------------------------- | -------------------------- | -------------------------------- | -------------------- | ------------------------------ | ------------------------------ | ------------------------------------- | ----------------------------------------- | ------------------------------------- |
| AgeYrs        | Age in years           | C1510829;C0001779                | Age-Years;Age              | UMLS;UMLS                        |                      |                                |                                |                                       |                                           |                                       |
| CaseContrlInd | Case control indicator | C0007328                         | Case-Control Studies       | UMLS                             | Case;Control;Unknown | Case;Control;Unknown           | 1;2;999                        | C1706256;C4553389;C0439673            | Clinical Study Case;Study Control;Unknown | UMLS;UMLS;UMLS                        |


## Semantic Search with SentenceTransformers Batch Queries
More documentation to come... Basic pipeline is described below:

### Subset/Embed/Upsert UMLS Metathesaurus for Pinecone vector database
#### Step 1: Subset local copy of UMLS Metathesaurus
#### Step 2: Embed UMLS CUI names and definitions and format metadata
#### Step 3: Upsert embeddings and metadata into Pinecone index

### Query UMLS Metathesaurus vector database with data dictionary embeddings
#### Step 1: Embed data dictionary fields
#### Step 2: Batch Query data dictionary against CUI names and definitions in Pinecone index
#### Step 3: Evaluate/Curate Results
#### Step 4: Create data dictionary based on curation


## Acknowledgements

The MetaMap API code included is from Will J Roger's repository --> https://github.com/lhncbc/skr_web_python_api

Special thanks to Olga Vovk, Henry Ogoe, and Sofia Syed for their guidance, feedback, and testing of this package.

## License

[MIT](https://choosealicense.com/licenses/mit/)
