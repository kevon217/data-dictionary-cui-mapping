# data-dictionary-cui-mapping

This package assists with mapping a user's data dictionary fields to [UMLS](https://www.nlm.nih.gov/research/umls/index.html) concepts. It is designed to be modular and flexible to allow for different configurations and use cases.

Roughly, the high-level steps are as follows:
- Configure yaml files
- Load in data dictionary
- Preprocess desired columns
- Query for UMLS concepts using any or all of the following pipeline modules:
  - **umls** (*UMLS API*)
  - **metamap** (*MetaMap API*)
  - **semantic_search** (*relies on access to a custom Pinecone vector database*)
  - **hydra_search** (*combines any combination of the above three modules*)
- Manually curate/select concepts in excel
- Create data dictionary file with new UMLS concept fields

## Prerequisites

- For UMLS API and MetaMap API, you will need to have an account with the UMLS API and/or MetaMap API. You can sign up for an account here: https://www.nlm.nih.gov/research/umls/index.html
- For Semantic Search with Pinecone, you will need to have an account with Pinecone. You can sign up for an account here: https://www.pinecone.io/. Please reach out to me if you would like temporary access to my Pinecone index to explore these embeddings.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install [data-dictionary-cui-mapping](https://pypi.org/project/data-dictionary-cui-mapping/) from PyPI or pip install from the [GitHub repo](https://github.com/kevon217/data-dictionary-cui-mapping). The project uses [poetry](https://python-poetry.org/) for packaging and dependency management.

```bash
pip install data-dictionary-cui-mapping
#pip install git+https://github.com/kevon217/data-dictionary-cui-mapping.git
```

## Input: Data Dictionary

Below is a sample data dictionary format (*.csv*) that can be used as input for this package:

| variable name | title                  | permissible value descriptions |
| ------------- | ---------------------- |--------------------------------|
| AgeYrs        | Age in years           |                                |
| CaseContrlInd | Case control indicator | Case;Control;Unknown           |

## Configuration Files

In order to run and customize these pipelines, you will need to create/edit yaml configuration files located in configs. Run configurations are saved and can be reloaded.

```bash
├───ddcuimap
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

## CUI Batch Query Pipelines


### STEP-1A: RUN BATCH QUERY PIPELINE
###### IMPORT PACKAGES

```python
# from ddcuimap.umls import batch_query_pipeline as umls_bqp
# from ddcuimap.metamap import batch_query_pipeline as mm_bqp
# from ddcuimap.semantic_search import batch_hybrid_query_pipeline as ss_bqp
from ddcuimap.hydra_search import batch_hydra_query_pipeline as hs_bqp

from ddcuimap.utils import helper
from omegaconf import OmegaConf
```
###### LOAD/EDIT CONFIGURATION FILES
```python
cfg_hydra = helper.compose_config.fn(overrides=["custom=hydra_base"])
# cfg_umls = helper.compose_config.fn(overrides=["custom=de", "apis=config_umls_api"])
cfg_mm = helper.compose_config.fn(overrides=["custom=de", "apis=config_metamap_api"])
cfg_ss = helper.compose_config.fn(
    overrides=[
        "custom=title_def",
        "semantic_search=embeddings",
        "apis=config_pinecone_api",
    ]
)

# # UMLS API CREDENTIALS
# cfg_umls.apis.umls.user_info.apiKey = ''
# cfg_umls.apis.umls.user_info.email = ''

# # MetaMap API CREDENTIALS
# cfg_mm.apis.metamap.user_info.apiKey = ''
# cfg_mm.apis.metamap.user_info.email = ''
#
# # Pinecone API CREDENTIALS
# cfg_ss.apis.pinecone.index_info.apiKey = ''
# cfg_ss.apis.pinecone.index_info.environment = ''

print(OmegaConf.to_yaml(cfg_hydra))
```

###### RUN BATCH QUERY PIPELINE
```python
# df_umls, cfg_umls = umls_bqp.run_umls_batch(cfg_umls)
# df_mm, cfg_mm = mm_bqp.run_mm_batch(cfg_mm)
# df_ss, cfg_ss = ss_bqp.run_hybrid_ss_batch(cfg_ss)
df_hydra, cfg_step1 = hs_bqp.run_hydra_batch(cfg_hydra, cfg_umls=None, cfg_mm=cfg_mm, cfg_ss=cfg_ss)

print(df_hydra.head())
```

### STEP-1B: **MANUAL CURATION STEP IN EXCEL*

###### CURATION/SELECTION
*see curation example in ***notebooks/examples_files/DE_Step-1_curation_keepCol.xlsx***

### STEP-2A: CREATE DATA DICTIONARY IMPORT FILE

###### IMPORT CURATION MODULES
```python
from ddcuimap.curation import create_dictionary_import_file
from ddcuimap.curation import check_cuis
from ddcuimap.utils import helper
```
###### CREATE DATA DICTIONARY IMPORT FILE

```python
cfg_step1 = helper.load_config.fn(helper.choose_file("Load config file from Step 1"))
df_dd = create_dictionary_import_file.create_dd_file(cfg_step1)
print(df_dd.head())
```

### STEP-2B: CHECK CUIS IN DATA DICTIONARY IMPORT FILE

###### CHECK CUIS
```python
cfg_step2 = helper.load_config.fn(helper.choose_file("Load config file from Step 2"))
df_check = check_cuis.check_cuis(cfg_step2)
print(df_check.head())
```

## Output: Data Dictionary + CUIs
Below is a sample modified data dictionary with curated CUIs after:
1. Running Steps 1-2 on **title** then taking the generated output dictionary file and;
2. Running Steps 1-2 again on **permissible value descriptions** to get the final output dictionary file.

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
