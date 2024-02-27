import io
import pandas as pd
import numpy as np
import requests
from typing import Tuple
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from mage_week4.utils.helpers.api_data_fetcher import fetch_data_api

@data_loader
def load_data_from_api(*args, **kwargs) -> Tuple[pd.DataFrame, str]:
    """Template for loading data from API
    """
    data = pd.DataFrame()

    needed_data = {
        'type' : 'yellow',
        'years': [2019, 2020]
    }

    for month in fetch_data_api(needed_data):
        data = pd.concat([data, month], ignore_index=True)

    return data, needed_data['type']


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
