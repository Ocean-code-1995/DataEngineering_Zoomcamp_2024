if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    if 'PUlocationID' in data.columns:
        data['PUlocationID'] = data['PUlocationID'].astype(float)
    else:
        data['PUlocationID'] = np.nan

    if 'DOlocationID' in data.columns:
        data['DOlocationID'] = data['DOlocationID'].astype(float)
    else:
        data['DOlocationID'] = np.nan

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefine