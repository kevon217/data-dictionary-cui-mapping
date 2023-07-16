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

from langchain.output_parsers import PydanticOutputParser, OutputFixingParser
from langchain.chat_models import ChatOpenAI


def run_llm_curation(cfg=None, **kwargs):
    if cfg is None:
        cfg = helper.load_config(helper.choose_file("Load config file from Step 1"))

    # LOAD/RETRIEVE CURATION DATAFRAME
    if not cfg.custom.create_dictionary_import_settings.curation_file_path:
        fp_curation = cur.get_curation_excel_file(
            "Select *_Step-1_curation_keepCol.xlsx file with curated CUIs"
        )
        cfg.custom.create_dictionary_import_settings.curation_file_path = fp_curation
    else:
        fp_curation = cfg.custom.create_dictionary_import_settings.curation_file_path

    if "kwargs" in locals():
        df_cur = kwargs.get("df_cur")
        if df_cur is None or df_cur.empty:
            df_cur = pd.read_csv(fp_curation, dtype="object")
    else:
        df_cur = pd.read_csv(fp_curation, dtype="object")

    # LOAD OPENAI API KEY
    load_dotenv(find_dotenv())
    try:
        OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
    except KeyError:
        raise Exception(
            "Could not load OpenAI API key. Make sure it's set in your environment variables."
        )

    # CREATE OUTPUT DIRECTORY
    dir_curation = helper.create_folder(
        Path(fp_curation).parent.joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_curation"
        )
    )

    # PREPROCESSING    #TODO: Fix this in prior pipeline so this can be removed
    df_cur["average_score"] = df_cur["average_score"].astype(float).round(3)
    df_cur["overall_rank"] = df_cur["overall_rank"].astype(int)

    # CREATE QA PROMPTS
    df_cur_filtered = []
    qa_prompts = []
    overall_rank = cfg.curation.preprocessing.overall_rank
    top_k_score = cfg.curation.preprocessing.top_k_score
    variables = df_cur[cfg.curation.preprocessing.variable_column].unique()
    for variable in variables:
        print(variable)
        df_filtered = filter_df_for_qa(df_cur, variable, overall_rank, top_k_score)
        df_cur_filtered.append(df_filtered)
        qa_prompt = create_qa_prompt_from_df(df_filtered)
        qa_prompts.append(qa_prompt)

    # SET UP LANGCHAIN
    model_name = cfg.curation.langchain.openai.model_name
    encoding_name = cfg.curation.langchain.openai.encoding_name
    temperature = cfg.curation.langchain.openai.temperature
    llm = OpenAI(model_name=model_name, temperature=temperature)
    output_parser = PydanticOutputParser(pydantic_object=ConceptQA)

    prompt = PromptTemplate(
        template=system_message_curator,
        input_variables=["qa_prompt", "example_qa"],
        partial_variables={
            "format_instructions": output_parser.get_format_instructions()
        },
    )

    inputs = []
    total_cost_prompts = 0
    for p in qa_prompts:
        _input = prompt.format_prompt(qa_prompt=p, example_qa=example_qa)
        num_tokens = num_tokens_from_string(
            _input.to_string(), encoding_name=encoding_name
        )
        cost = round(
            openaipricing(
                num_tokens, cfg.curation.langchain.openai.pricing_model, chatgpt=True
            ),
            3,
        )
        total_cost_prompts += cost
        print(f"# of tokens: {num_tokens} ({cost} USD)")
        inputs.append(_input)
    print(f"Total cost of prompts: {total_cost_prompts} USD")

    outputs = []
    with get_openai_callback() as cb:
        for i in inputs:
            output = llm(i.to_string())
            print(cb)
            try:
                answers = output_parser.parse(output).Answers
            except:
                new_parser = OutputFixingParser.from_llm(
                    parser=output_parser, llm=ChatOpenAI()
                )
                parsed_output = new_parser.parse(output)
                answers = parsed_output.Answers
            print(answers)
            outputs.append(answers)

    for df in df_cur_filtered:
        df["Reasoning"] = np.nan
        df["Confidence"] = np.nan
        df["keep"] = ""  # Add a 'keep' column filled with empty strings

    # After you get the model's output...
    for i, answers in enumerate(outputs):
        df = df_cur_filtered[i]
        for answer, details in answers.items():
            # Convert the answer number back to an index
            index = int(answer) - 1
            # Annotate the dataframe with the model's reasoning and confidence
            df.loc[df.index[index], "Reasoning"] = details.Reasoning
            df.loc[df.index[index], "Confidence"] = details.Confidence
            df.loc[df.index[index], "keep"] = 1  # Mark this row with an 1 to keep it

    df_final = pd.concat(df_cur_filtered)
    df_final.to_csv(
        dir_curation.joinpath(
            f"{cfg.custom.curation_settings.file_settings.directory_prefix}_curation.csv"
        ),
        index=False,
    )
    # TODO: merge llm curated results back with original
    # df_final.set_index('original_index', inplace=True)
    helper.save_config(cfg, dir_curation, "config_curation.yaml")

    return df_final, cfg


if __name__ == "__main__":
    cfg = helper.load_config(helper.choose_file("Load config file from Step 1"))
    df_final, cfg = run_llm_curation(cfg)
