"""

General helper functions for UI and configuration components of scripts.

"""

import os
import tkinter as tk
from tkinter import filedialog

from hydra import compose, initialize
from omegaconf import OmegaConf
from prefect import task

# LOAD CONFIGS


@task(name="Composing config file")
def compose_config(config_path="../configs", config_name="config", overrides=None):
    """Load configurations from the file `config.yaml` under the `config` directory and specify overrides"""
    with initialize(version_base=None, config_path=config_path):
        config = compose(config_name=config_name, overrides=overrides)
    return config


# SAVE CONFIGS


@task(name="Saving config file")
def save_config(dir_step1, cfg):
    """Save Omegaconf configuration file"""

    fp = os.path.join(dir_step1, "config.yaml")
    with open(fp, "w") as fp:
        OmegaConf.save(config=cfg, f=fp.name)
        print("Saving config file")


@task(name="Loading config file")
def load_config(filepath):
    """Load OmegaConf configuration file"""

    with open(filepath, "r") as fp:
        loaded = OmegaConf.load(fp.name)
    print("Loading config file")
    return loaded


# FOLDER/DIRECTORY FUNCTIONS


@task(name="Creating new folder")
def create_folder(folder_path):
    """Will create new folder and append numbers incrementally if folder_path already exists"""

    adjusted_folder_path = folder_path
    folder_found = os.path.isdir(adjusted_folder_path)
    counter = 0
    while folder_found:
        counter = counter + 1
        adjusted_folder_path = folder_path + " (" + str(counter) + ")"
        folder_found = os.path.isdir(adjusted_folder_path)
    os.mkdir(adjusted_folder_path)
    print(f"Folder created: {adjusted_folder_path}")
    return adjusted_folder_path


def manage_tk_dialogbox(tk):
    """Brings tk dialogbox to the front of screen"""

    root = tk.Tk()
    root.withdraw()
    root.focus_force()
    root.attributes("-topmost", True)
    return root


@task(name="Choosing input file")
def choose_input_file(prompt: str):
    """Opens up tk filedialog box to allow user to choose a local file"""

    root = manage_tk_dialogbox(tk)
    fp = filedialog.askopenfilename(parent=root, title=prompt)
    print(f"Input file chosen: {fp}")
    return fp
