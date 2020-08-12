import pprint

import pytest

from aggregator.aggregate import AggCSV

def test_aggcsv_instantiate():
    """ test instantiation of AggCSV object """

def test_error_count_decorator(tmpdir, supply_generalSchema_data):
    """ test _error_count decorator function tracks errors
    correctly across functions """

    # create single csv of test data
    df = supply_generalSchema_data[0]
    df_loc = str(tmpdir.join('test_csv0'))
    df.to_csv(df_loc, index=False)

    # create AggCSV object
    a = AggCSV('general', df_loc)
    a.validate()
    assert a.rows_w_errors == {3,4}
    assert a.distinct_cols_w_errors == {'Redirect Link', 'Cost Per Ad Click'}

def test_captured_errors(tmpdir, supply_generalSchema_data):
    """ test that errors that are captured are expected """

    # create single csv of test data
    df = supply_generalSchema_data[1]
    df_loc = str(tmpdir.join('test_csv0'))
    df.to_csv(df_loc, index=False)

    # results data
    results = [
        ['test_csv0', 'Redirect Link', 0, 'valid_redirect_link', 'wont work'],
        ['test_csv0', 'Zipcode', 0, 'valid_zipcode', '7876510']
    ]

    # create AggCSV object
    a = AggCSV('general', df_loc)
    a.validate()
    for i, err in enumerate(a.errors):
        err[0] = err[0].split('/')[-1]
        # exclude comparing dataframe as last entry
        assert err[:-1] == results[i]
