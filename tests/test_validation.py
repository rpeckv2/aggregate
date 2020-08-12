from pprint import pprint

import pytest
import numpy as np
import pandas as pd

from aggregator.aggregate import AggCSV
from aggregator.schema import Validation, GeneralSchema, SchemaTypeNotDeclared

def dummy_df():
    """ empty df """
    return pd.DataFrame({'dummy':[1,2]})

def test_validation_instantiate():
    """ test Validation object is instantiated """
    v = Validation()
    assert type(v) == Validation

def test_get_schema_attrs(supply_testSchema):
    """ test attrs from classes that inherit Validation
    are successfully retreived """

    ts = supply_testSchema()
    tm = {k:v for k,v in ts.get_schema_attrs()}
    
    results = {'test_attr1':{
                'name':'Test Attr 1',
                'type':str,
                'nullable':True,
                'valiations':[]
                }
            }
    assert len(tm) == 4
    assert results['test_attr1']['name'] == tm['test_attr1']['name']

def test_SchemaTypeNotDeclared_error(tmpdir, supply_testSchema_data):
    """ test SchemaTypeNotDeclared is raised """

    # create single csv of test data
    df = supply_testSchema_data[0]
    df_loc = str(tmpdir.join('test_csv0'))
    df.to_csv(df_loc, index=False)

    # create AggCSV object
    a = AggCSV('test', df_loc)

    with pytest.raises(SchemaTypeNotDeclared, match=r'No type declared for Test Attr 4'):
        _ = a.validate()

def test_validate_type_str(tmpdir, supply_generalSchema_data):
    """ test that type specified in child class
    can be successfully converted """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'provider_name']
    data = [5.0, ga[0], 'csv_path', dummy_df()]
    val = gs.validate_type(*data)

    assert val == '5.0'

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'provider_name']
    data = ['5.0', ga[0], 'csv_path', dummy_df()]
    val = gs.validate_type(*data)

    assert val == '5.0'

def test_validate_type_float():
    """ test that type specified in child class
    can be successfully converted """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
    data = ['1.0', ga[0], 'csv_path', dummy_df()]
    val = gs.validate_type(*data)
    
    assert val == 1.0

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
    data = [1.0, ga[0], 'csv_path', dummy_df()]
    val = gs.validate_type(*data)
    
    assert val == 1.0

def test_validate_null_whitespace():
    """ test null values can be correctly identified 
    and flagged based on child class attribute map """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
    data = [' \n', ga[0], 'csv_path', dummy_df()]
    val = gs.validate_null(*data)

    assert val == False

def test_validate_null_numpy_nan():
    """ test null values can be correctly identified 
    and flagged based on child class attribute map """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
    data = [np.nan, ga[0], 'csv_path', dummy_df()]
    val = gs.validate_null(*data)

    assert val == False

def test_validate_null_return_nan():
    """ test null values can be correctly identified 
    and flagged based on child class attribute map """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'phone_number']
    data = [np.nan, ga[0], 'csv_path', dummy_df()]
    val = gs.validate_null(*data)

    assert np.isnan(val)

def test_validate_not_null():
    """ test null values can be correctly identified 
    and flagged based on child class attribute map """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'phone_number']
    data = [55, ga[0], 'csv_path', dummy_df()]
    val = gs.validate_null(*data)

    assert val == 55

# def test_valid_provider_name():
#     """ test provider name can be validated and standardized """

# def test_valid_campaign_id():
#     """ test campaign id can be validated and standardized """

# def test_valid_address():
#     """ test a provided address can be validated and standardized """

def test_valid_cost_per_ad_click():
    """ test cost per ad click column can be validated based
    on child class attribute map """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
    data = [55, ga[0], 'csv_path', dummy_df()]
    val = gs.valid_cost_per_ad_click(*data)
    print(val)
    assert val == 55

def test_valid_redirect_link():
    """ test redirect link is a valid URL """

    gs = GeneralSchema()
    ga = [v for k,v in gs.get_schema_attrs() if k == 'redirect_link']
    data = ['www.google.com', ga[0], 'csv_path', dummy_df()]
    val = gs.valid_redirect_link(*data)
    print(val)
    assert 'www.google.com' == val

def test_valid_phone_number():
    """ test that phone number is in valid format and 
    can be standardized """
    numbers = ['(512)239-9784', '512239-9784', '5122399784', '512239.9784',
                '512239,9784', '239-9784', '2399784', '239.9784','399754',
                '(5122399784', 5122399784, '0000000000']
    validity = ['(512)239-9784', '(512)239-9784', '(512)239-9784', '(512)239-9784',
                '(512)239-9784', '239-9784', '239-9784', '239-9784', False,
                '(512)239-9784', '(512)239-9784', '(000)000-0000']

    for i, number in enumerate(numbers):
        gs = GeneralSchema()
        ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
        data = [number, ga[0], 'csv_path', dummy_df()]
        val = gs.valid_phone_number(*data)
        print(val, number)
        assert val == validity[i]

def test_valid_zipcode():
    """ test that zip code code is in valid format and can be 
    standardized """
    numbers = [78752, '78752', '78752-2534', '787', 7654, '787522534', 787651089]
    validity = ['78752', '78752', '78752-2534', False, False, '78752-2534', '78765-1089']

    for i, number in enumerate(numbers):
        gs = GeneralSchema()
        ga = [v for k,v in gs.get_schema_attrs() if k == 'cost_per_ad_click']
        data = [number, ga[0], 'csv_path', dummy_df()]
        val = gs.valid_zipcode(*data)
        print(val, number)
        assert val == validity[i]
