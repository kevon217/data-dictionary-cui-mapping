"""

General functions for UI components of scripts.

"""

import os
import tkinter as tk
from tkinter import filedialog

# FOLDER/DIRECTORY FUNCTIONS


def create_folder(folder_path):  # creates new folders every time script is run
    adjusted_folder_path = folder_path
    folder_found = os.path.isdir(adjusted_folder_path)
    counter = 0
    while folder_found == True:
        counter = counter + 1
        adjusted_folder_path = folder_path + " (" + str(counter) + ")"
        folder_found = os.path.isdir(adjusted_folder_path)
    os.mkdir(adjusted_folder_path)
    return adjusted_folder_path


def manage_tk_dialogbox(tk):
    root = tk.Tk()
    root.withdraw()
    root.focus_force()
    root.attributes("-topmost", True)
    return root


def choose_input_file(prompt: str):
    root = manage_tk_dialogbox(tk)
    fp = filedialog.askopenfilename(parent=root, title=prompt)
    return fp
