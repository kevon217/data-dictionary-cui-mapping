"""

Various functions for checking and manipulating vectors and tokens.

"""

from prefect import task
import numpy as np
from transformers import AutoTokenizer

# VECTOR OPERATIONS


def scale_0_1(x, to_float=True):
    """Normalize a vector to a range of 0 to 1"""

    x_norm_0_1 = (x - np.min(x)) / (np.max(x) - np.min(x))
    if to_float:
        x_norm_0_1 = [float(np_float) for np_float in x_norm_0_1]

    return x_norm_0_1


def normalize_unit_length(vec):
    """Normalize a vector to unit length"""

    vec_norm = vec / np.linalg.norm(vec)
    if (
        type(vec) == list
    ):  # TODO: need to consider other types/scenarios. when will list be used vs. numpy array
        vec_norm = [float(np_float) for np_float in vec_norm]
    return vec_norm


# VECTOR CHECKS


def test_unit_length(vec):
    """Test if a vector is noramlized to unit length"""

    is_unit_len = np.isclose(np.linalg.norm(vec), 1.0)
    return is_unit_len


def vec_min_max(vec):
    """Find the min and max of a vector"""

    min_val = np.min(vec)
    max_val = np.max(vec)
    return min_val, max_val


def dict_min_max(dictionary):
    """Find the min and max values of a dictionary and get the corresponding values"""

    key_max = max(dictionary.keys(), key=(lambda k: dictionary[k]))
    key_min = min(dictionary.keys(), key=(lambda k: dictionary[k]))
    value_max = dictionary[key_max]
    value_min = dictionary[key_min]
    return key_min, value_min, key_max, value_max


# TOKEN CHECKS


@task(name="Calculate Number of Tokens Using Model")
def calc_num_tokens(df, columns, model_name):
    """Calculate the number of tokens generated for each input using a given model"""

    tokenizer = AutoTokenizer.from_pretrained(model_name)
    for col in columns:
        df[f"{col}_token_count"] = df[col].apply(lambda x: len(tokenizer.tokenize(x)))
    return df
