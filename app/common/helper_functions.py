import json
import os

def file_reader(FilePath):
    with open(FilePath,"r") as f:
        contents = f.read()
        return contents