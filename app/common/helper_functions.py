import json
import os
from test import debug as masterdebug

def file_reader(FilePath):
    with open(FilePath) as f:
        contents = f.read()
        return contents