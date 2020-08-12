import shutil

import pytest
import pandas as pd

from aggregator.schema import Validation

class TestSchema(Validation):
    """ test schema class that inherits the Validation super class """
    test_attr1 = {
        'name':'Test Attr 1',
        'type':str,
        'nullable':True,
        'validations':[]
    }
    test_attr2 = {
        'name':'Test Attr 2',
        'type':float,
        'nullable':False,
        'validations':[]
    }
    test_attr3 = {
        'name':'Test Attr 3',
        'type':str,
        'nullable':False,
        'validations':[]
    }
    test_attr4 = {
        'name':'Test Attr 4',
        'nullable':False,
        'validations':[]
    }

@pytest.fixture(scope='session')
def tempdir(tmpdir_factory):
    """ create temp folder for all data files """
    tmpdir = tmpdir_factory.mktemp('data')
    yield tmpdir
    shutil.rmtree(str(tmpdir))

@pytest.fixture
def supply_testSchema():
    """ return TestSchema object """
    return TestSchema

@pytest.fixture
def supply_testSchema_data():
    """ pytest fixture to supply data to test files """
    df1 = testSchema_data1()
    return [df1]

@pytest.fixture
def supply_generalSchema_data():
    """ pytest fixture to supply data to test files """
    df1 = generalSchema_data1()
    df2 = generalSchema_data2()
    df3 = generalSchema_data3()
    df4 = generalSchema_data4()
    df5 = generalSchema_data5()
    df6 = generalSchema_data6()
    return [df1, df2, df3, df4, df5, df6]

# ---------------------------------------------------
# Test data for TestSchema
# ---------------------------------------------------

def testSchema_data1():
    """ return a dataframe of test data to later be written as a csv """
    data = {
        'Test Attr 1':['test1', None, 'test3', 'test4', 'test5'],
        'Test Attr 2':[1, None, 3, 4.0, 'test5'],
        'Test Attr 3':['test6', None, 'test7', 'test8', 'test9'],
        'Test Attr 4':['test10', None, 'test11', 'test12', 'test13'],
    }
    df = pd.DataFrame(data)
    return df

# ---------------------------------------------------
# Test Data for GeneralSchema
# ---------------------------------------------------

def generalSchema_data1():
    """ return a dataframe of test data to later be written as a csv 
    includes fields that will not be included in output csv file"""
    data = {
        'Provider Name':['test1', 'test2', 'test3', 'test4', 'test5'],
        'CampaignID':['1234', '5678', '234', '234234', 'asf234'],
        'Cost Per Ad Click':[22, 1, 1.0, 3.4, None],
        'Redirect Link':['www.google.com', 'yahoo.com', 'facebook.com', 'wont work', 'google.com'],
        'Phone Number':[5555555555, '5555555555', 5555555, 5555555, '0000000000'],
        'Address':['1234 Dr.', '1234 Street', '5234 Way', '22 55 drive', 'random street'],
        'Zipcode':[33345, 111111333, 66543, 88991, '99011'],
        'NotIncluded':[1, None, 3, 4.0, 'test5'],
    }
    df = pd.DataFrame(data)
    return df

def generalSchema_data2():
    """ return a dataframe of test data to later be written as a csv 
    includes fields that will not be included in output csv file"""
    data = {
        'Provider Name':['test1'],
        'CampaignID':[1],
        'Cost Per Ad Click':[45],
        'Redirect Link':['wont work'],
        'Phone Number':[5555555555],
        'Address':['1234 Mary Avenue'],
        'Zipcode':[7876510],
        'NotIncluded':['test'],
    }
    df = pd.DataFrame(data)
    return df

def generalSchema_data3():
    """ return a dataframe of test data to later be written as a csv 
    includes fields that will not be included in output csv file"""
    data = {
        'Provider Name':['test1', 'test2', 'test3', 'test4', 'test5'],
        'CampaignID':['1234', '5678', '234', '234234', 'asf234'],
        'Cost Per Ad Click':[22, 1, 1.0, 3.4, None],
        'Redirect Link':['www.google.com', 'yahoo.com', 'facebook.com', 'wont work', 'google.com'],
        'Phone Number':[5555555555, '5555555555', 5555555, 5555555, '0000000000'],
        'Address':['1234 Dr.', '1234 Street', '5234 Way', '22 55 drive', 'random street'],
        'Zipcode':[33345, 111111333, 66543, 88991, '99011'],
        'NotIncluded':[1, None, 3, 4.0, 'test5'],
    }
    df = pd.DataFrame(data)
    return df

def generalSchema_data4():
    """ return a dataframe of test data to later be written as a csv 
    includes fields that will not be included in output csv file"""
    data = {
        'Provider Name':['test1', 'test2', 'test3', 'test4', 'test5'],
        'CampaignID':['1234', '5678', '234', '234234', 'asf234'],
        'Cost Per Ad Click':[22, 1, 1.0, 3.4, None],
        'Redirect Link':['www.google.com', 'yahoo.com', 'facebook.com', 'wont work', 'google.com'],
        'Phone Number':[5555555555, '5555555555', 5555555, 5555555, '0000000000'],
        'Address':['1234 Dr.', '1234 Street', '5234 Way', '22 55 drive', 'random street'],
        'Zipcode':[33345, 111111333, 66543, 88991, '99011'],
        'NotIncluded':[1, None, 3, 4.0, 'test5'],
    }
    df = pd.DataFrame(data)
    return df

def generalSchema_data5():
    """ return a dataframe of test data to later be written as a csv 
    includes fields that will not be included in output csv file"""
    data = {
        'Provider Name':['test1', 'test2', 'test3', 'test4', 'test5'],
        'CampaignID':['1234', '5678', '234', '234234', 'asf234'],
        'Cost Per Ad Click':[22, 1, 1.0, 3.4, None],
        'Redirect Link':['www.google.com', 'yahoo.com', 'facebook.com', 'wont work', 'google.com'],
        'Phone Number':[5555555555, '5555555555', 5555555, 5555555, '0000000010'],
        'Address':['1234 Dr.', '1234 Street', '5234 Way', '22 55 drive', 'random street'],
        'Zipcode':[33345, 111111333, 66543, 88991, '99011'],
        'NotIncluded':[1, None, 3, 4.0, 'test5'],
    }
    df = pd.DataFrame(data)
    return df

def generalSchema_data6():
    """ return a dataframe of test data to later be written as a csv 
    includes fields that will not be included in output csv file"""
    data = {
        'Provider Name':['test1'],
        'CampaignID':[1],
        'Cost Per Ad Click':[45],
        'Redirect Link':['www.willwork.com'],
        'Phone Number':[5555555555],
        'Address':['1234 Peter Parker Place'],
        'Zipcode':[787651089],
        'NotIncluded':['test'],
    }
    df = pd.DataFrame(data)
    return df


    