'''Misc utils throughout the repo.'''

from typing import Dict
from os import path
import re

import yaml

def load_yaml(filepath: str) -> Dict:
    # Validate passed filepath.
    filepath_without_ext, ext = path.splitext(filepath)
    assert re.search('^\.y(a)?ml', ext) is not None, \
        f'Filepath must be yaml, found: {ext}: {filepath}'
    assert path.exists(filepath), f'File not found: {filepath}'

    # Load yaml.
    result = None
    with open(filepath, 'r') as file:
        try:
            result = yaml.safe_load(file)
        except Exception as e:
            raise ValueError(f'Error parsing yaml at {filepath}: {e}') from e
    
    return result