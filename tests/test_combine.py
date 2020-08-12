import pytest
import pandas as pd

from aggregator.aggregate import Combine
from aggregator.schema import GeneralSchema

# many of these tests are not strictly unit tests, many are used to spot
# check that the output and generated CSV files "look" correct

def test_combine_instantiate():
    """ create an instance of Combine """
    c = Combine('test')
    assert type(c) == Combine

def test_combine_csvs(tmpdir, supply_generalSchema_data):
    """ test combining and cleaning multiple csv files
    based on the GeneralSchema """

    # create csvs of test data
    for i, df in enumerate(supply_generalSchema_data):
        file_name = f'test_csv_{i}.csv'
        df_loc = str(tmpdir.join(file_name))
        end = '\r\n' if i%2==0 else '\n'
        df.to_csv(df_loc, index=False, line_terminator=end)
    
    # combine csvs
    csv_loc = '/'.join(df_loc.split('/')[:-1])
    gs = GeneralSchema()
    cols = [v['name'] for k,v in gs.get_schema_attrs()]

    c = Combine('general', csv_loc)
    c.combine_csvs()
    c.print_summary()
    assert len(c.master_df) == 13

def test_write_combined_csv(tmpdir, supply_generalSchema_data):
    """ test combined csv is successfully written to working directory"""

    # create csvs of test data
    for i, df in enumerate(supply_generalSchema_data):
        file_name = f'test_csv_{i}.csv'
        df_loc = str(tmpdir.join(file_name))
        end = '\r\n' if i%2==0 else '\n'
        df.to_csv(df_loc, index=False, line_terminator=end)
    
    # combine  and write csvs
    csv_loc = '/'.join(df_loc.split('/')[:-1])
    gs = GeneralSchema()
    cols = [v['name'] for k,v in gs.get_schema_attrs()]
    output_loc = str(tmpdir.join('combined_output.csv'))
    c = Combine('general', csv_loc)
    c.combine_csvs()
    c.write_combined_csv(output_loc)
    c.print_summary()

def test_write_combined_csv_single(tmpdir, supply_generalSchema_data):
    """ test combined csv is successfully written to working directory"""

    # create csvs of test data
    df = supply_generalSchema_data[0]
    i = 0
    file_name = f'test_csv_{i}.csv'
    df_loc = str(tmpdir.join(file_name))
    end = '\r\n' if i%2==0 else '\n'
    df.to_csv(df_loc, index=False, line_terminator=end)
    
    # combine  and write csvs
    csv_loc = '/'.join(df_loc.split('/')[:-1])
    gs = GeneralSchema()
    cols = [v['name'] for k,v in gs.get_schema_attrs()]
    output_loc = str(tmpdir.join('combined_output.csv'))
    c = Combine('general', csv_loc)
    c.combine_csvs()
    c.write_combined_csv(output_loc)
    c.print_summary()

    # read in and compare
    # result = pd.read_csv(output_loc)
    # assert len(result) == len(c.master_df)
    # assert result.columns.tolist().sort() == c.master_df.columns.tolist().sort()
    # assert result.shape == c.master_df.shape
    # assert result.isna().sum().sum() == c.master_df.isna().sum().sum()

def test_combine_hard_csv_files(tmpdir):
    """ test combining saved csv files from extracted zip file """

    # combine  and write csvs
    csv_loc = './tests/csv_files'
    gs = GeneralSchema()
    cols = [v['name'] for k,v in gs.get_schema_attrs()]
    output_loc = str(tmpdir.join('combined_output.csv'))
    c = Combine('general', csv_loc)
    c.combine_csvs()
    c.write_combined_csv(output_loc)
    c.print_summary()

# def test_write_combined_1000_csv(tmpdir, supply_generalSchema_data):
#     """ test 1000 combined csvs are successfully written to working directory"""

#     # create csvs of test data
#     for j in range(1000):
#         for i, df in enumerate(supply_generalSchema_data):
#             file_name = f'test_csv_{j}{i}.csv'
#             df_loc = str(tmpdir.join(file_name))
#             end = '\r\n' if i%2==0 else '\n'
#             df.to_csv(df_loc, index=False, line_terminator=end)
    
#     # combine  and write csvs
#     csv_loc = '/'.join(df_loc.split('/')[:-1])
#     gs = GeneralSchema()
#     cols = [v['name'] for k,v in gs.get_schema_attrs()]
#     output_loc = str(tmpdir.join('combined_output.csv'))
#     c = Combine('general', csv_loc)
#     c.combine_csvs()
#     c.write_combined_csv(output_loc)
#     c.print_summary()

def test_print_errors(tmpdir, supply_generalSchema_data):
    """ test print output for found errors across all combined
    csv files """
    # create csvs of test data
    for i, df in enumerate(supply_generalSchema_data):
        file_name = f'test_csv_{i}.csv'
        df_loc = str(tmpdir.join(file_name))
        end = '\r\n' if i%2==0 else '\n'
        df.to_csv(df_loc, index=False, line_terminator=end)
    
    # combine csvs
    csv_loc = '/'.join(df_loc.split('/')[:-1])
    gs = GeneralSchema()
    cols = [v['name'] for k,v in gs.get_schema_attrs()]
    output_loc = str(tmpdir.join('combined_output.csv'))
    c = Combine('general', csv_loc)
    c.combine_csvs()

    # print errors
    c.print_summary()


