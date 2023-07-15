from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import numpy as np

from ddcuimap.utils import helper as helper
from ddcuimap.curation import cur_logger, log, copy_log
from ddcuimap.curation.utils import curation_functions as cur
from ddcuimap.curation.chains.utils.format_qa import (
    ConceptQA,
    filter_df_for_qa,
    create_qa_prompt_from_df,
)
from ddcuimap.curation.chains.system_prompts.curator import (
    system_message_curator,
    example_qa,
)
from ddcuimap.curation.chains.utils.token_functions import (
    num_tokens_from_string,
    openaipricing,
)

from langchain import PromptTemplate, OpenAI, LLMChain
from langchain.callbacks import get_openai_callback

from langchain.output_parsers import PydanticOutputParser

load_dotenv(find_dotenv())

try:
    OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
except KeyError:
    raise Exception(
        "Could not load OpenAI API key. Make sure it's set in your environment variables."
    )


# LOAD CONFIG FILES
cfg_cur = helper.compose_config(
    overrides=[
        "custom=title_def",
        "curation=chain_setup",
        "semantic_search=embeddings",
        "apis=config_pinecone_api",
    ]
)
cfg_ss = helper.load_config(helper.choose_file("Load config file from Step 1"))


# LOAD CURATION FILE
if not cfg_ss.custom.create_dictionary_import_settings.curation_file_path:
    fp_curation = cur.get_curation_excel_file(
        "Select *_Step-1_curation_keepCol.xlsx file with curated CUIs"
    )
    cfg_ss.custom.create_dictionary_import_settings.curation_file_path = fp_curation
else:
    fp_curation = cfg_ss.custom.create_dictionary_import_settings.curation_file_path

df = pd.read_csv(fp_curation, dtype="object")
df["average_score"] = df["average_score"].astype(float).round(3)
df["overall_rank"] = df["overall_rank"].astype(int)

# CREATE OUTPUT DIRECTORY
dir_curation = helper.create_folder(
    Path(fp_curation).parent.joinpath(
        f"{cfg_ss.custom.curation_settings.file_settings.directory_prefix}_curation"
    )
)

# CREATE QA PROMPTS
dfs_filtered = []
qa_prompts = []
overall_rank = 5
top_k_score = 5
variables = df["variable name"].unique()
for variable in variables:
    print(variable)
    df_filtered = filter_df_for_qa(df, variable, overall_rank, top_k_score)
    dfs_filtered.append(df_filtered)
    qa_prompt = create_qa_prompt_from_df(df_filtered)
    qa_prompts.append(qa_prompt)

# SET UP LANGCHAIN
model_name = "text-davinci-003"
encoding_name = "p50k_base"
temperature = 0.0
llm = OpenAI(model_name=model_name, temperature=temperature)
output_parser = PydanticOutputParser(pydantic_object=ConceptQA)

prompt = PromptTemplate(
    template=system_message_curator,
    input_variables=["qa_prompt", "example_qa"],
    partial_variables={"format_instructions": output_parser.get_format_instructions()},
)

inputs = []
total_cost_prompts = 0
for p in qa_prompts:
    _input = prompt.format_prompt(qa_prompt=p, example_qa=example_qa)
    num_tokens = num_tokens_from_string(_input.to_string(), encoding_name=encoding_name)
    cost = round(openaipricing(num_tokens, "davinci", chatgpt=True), 3)
    total_cost_prompts += cost
    print(f"# of tokens: {num_tokens} ({cost} USD)")
    inputs.append(_input)
print(f"Total cost of prompts: {total_cost_prompts} USD")


outputs = []
with get_openai_callback() as cb:
    for i in inputs:
        output = llm(i.to_string())
        print(cb)
        answers = output_parser.parse(output).Answers
        print(answers)
        outputs.append(answers)


for df in dfs_filtered:
    df["Reasoning"] = np.nan
    df["Confidence"] = np.nan
    df["keep"] = ""  # Add a 'keep' column filled with empty strings

# After you get the model's output...
for i, answers in enumerate(outputs):
    df = dfs_filtered[i]
    for answer, details in answers.items():
        # Convert the answer number back to an index
        index = int(answer) - 1
        # Annotate the dataframe with the model's reasoning and confidence
        df.loc[df.index[index], "Reasoning"] = details.Reasoning
        df.loc[df.index[index], "Confidence"] = details.Confidence
        df.loc[df.index[index], "keep"] = 1  # Mark this row with an 1 to keep it

df_final = pd.concat(dfs_filtered)
df_final.to_csv(
    dir_curation.joinpath(
        f"{cfg_ss.custom.curation_settings.file_settings.directory_prefix}_curation.csv"
    ),
    index=False,
)
# df_final.set_index('original_index', inplace=True)
