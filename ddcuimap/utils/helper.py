"""

General helper functions for UI and configuration components of scripts.

"""
import logging
import os
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
from hydra import compose, initialize
from omegaconf import OmegaConf
from omegaconf.dictconfig import DictConfig
from typing import List, Optional

from ddcuimap.utils.decorators import log
from ddcuimap.utils import logger

# CONFIG FILE FUNCTIONS


@log(msg="Composing config file")
def compose_config(
    config_path: str = "../configs",
    config_name: str = "config",
    overrides: Optional[List[str]] = None,
) -> DictConfig:
    """Load configurations from the file `config.yaml` under the `config` directory and specify overrides"""
    with initialize(
        config_path=config_path
    ):  # removed version_base=None when upgrading to hydra 1.1.0
        config = compose(config_name=config_name, overrides=overrides)
    return config


@log(msg="Saving config file")
def save_config(cfg, dir_step1, filename=None):
    """Save Omegaconf configuration file"""
    if filename:
        fp = Path(dir_step1 / filename)
    else:
        fp = Path(dir_step1 / "config.yaml")
    with open(fp, "w") as fp:
        OmegaConf.save(config=cfg, f=fp.name)


@log(msg="Loading config file")
def load_config(filepath):
    """Load OmegaConf configuration file"""

    with open(filepath, "r", encoding="utf-8") as fp:
        loaded = OmegaConf.load(fp.name)
    return loaded


# FOLDER/DIRECTORY FUNCTIONS


@log(msg="Creating new folder")
def create_folder(folder_path):
    """Will create new folder and append numbers incrementally if folder_path already exists"""

    if type(folder_path) == str:
        adjusted_folder_path = folder_path
        folder_found = os.path.isdir(adjusted_folder_path)
        counter = 0
        while folder_found:
            counter = counter + 1
            adjusted_folder_path = folder_path + " (" + str(counter) + ")"
            folder_found = os.path.isdir(adjusted_folder_path)
        os.mkdir(adjusted_folder_path)
        logger.info(f"Folder created: {adjusted_folder_path}")
    elif isinstance(folder_path, Path):
        # using pathlib and not os check if folder_path already exists, append numbers incrementally
        adjusted_folder_path = folder_path
        folder_found = folder_path.exists()
        counter = 0
        while folder_found:
            counter = counter + 1
            adjusted_folder_path = folder_path.parent / (
                folder_path.name + " (" + str(counter) + ")"
            )
            folder_found = adjusted_folder_path.exists()
        adjusted_folder_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Folder created: {adjusted_folder_path}")
    else:
        raise TypeError("folder_path must be a string or pathlib Path object")

    return adjusted_folder_path


def manage_tk_dialogbox(tk):
    """Brings tk dialogbox to the front of screen"""

    root = tk.Tk()
    root.withdraw()
    root.focus_force()
    root.attributes("-topmost", True)
    return root


@log(msg="Choosing input file")
def choose_file(prompt: str):
    """Opens up tk filedialog box to allow user to choose a local file"""

    root = manage_tk_dialogbox(tk)
    fp = filedialog.askopenfilename(parent=root, title=prompt)
    logger.info(f"File chosen: {fp}")
    return fp


@log(msg="Choosing input directory")
def choose_dir(prompt: str):
    """Opens up tk filedialog box to allow user to choose a local directory"""

    root = manage_tk_dialogbox(tk)
    dp = filedialog.askdirectory(parent=root, title=prompt)
    logger.info(f"Directory chosen: {dp}")
    return dp
