import argparse
import os
from datetime import datetime
from pprint import pprint

import pandas as pd
pd.options.display.width=None

from aggregator.utilities import schema_map


class AggCSV:
    """
    Parsed and validated Insurance Aggregator Inc CSV.
    Creates pandas dataframe from provided csv to validate.
    """

    def __init__(self, insurance_type = None, csv = None, schema_cols =None):
        self.df = self.pandas_read_csv(csv) if csv else None
        self.insurance_type = insurance_type
        self.csv_path = csv
        self.schema_cols = schema_cols
        self.rows_w_errors = set()
        self.distinct_cols_w_errors = set()
        self.errors = None

    def pandas_read_csv(self, csv, sep=',', encoding='utf-8', dtype=str):
        """
        Read in CSV as a pandas dataframe for manipulation
        lineterminator is not specified for csvs to pandas dataframe currently

        dtype=str prevents string to number pandas conversions
        """
        return pd.read_csv(csv, sep=sep, encoding=encoding, dtype=dtype)

    def validate(self):
        """
        Apply standard and defined column validations for specified schema class.
        Record error data in Schema object and return
        """
        
        schm = schema_map(self.insurance_type)
        for _, v in schm.get_schema_attrs():
            col = v['name']
            # apply standard validations
            for validation in schm.get_std_validations():
                self.df[col] = self.df[col].apply(lambda x: validation(x, v, self.csv_path, self.df))
                schm.set_row(0)
            # apply specific validations
            for validation in v['validations']:
                self.df[col] = self.df[col].apply(lambda x: validation(schm, x, v, self.csv_path, self.df))
                schm.set_row(0)

        # retrieve data from schema object and remove errors rows from self.df
        self.rows_w_errors = schm.get_rows_w_errors()
        self.distinct_cols_w_errors = schm.get_distinct_cols_w_errors()
        self.errors = schm.get_errors()
        self.df.drop(index=self.rows_w_errors, inplace=True)

    def _just_str(self, my_str, val):
        """ justify a number for use in self.print_errors """
        str_len = len(my_str)
        just = 35
        out = my_str  + ' '*(just-str_len) + ': ' + str(val)
        return out

    def _csv_summary_info(self, rows, cols, line):
        """ print out formatted csv summary error info """
        print(self._just_str('CSV Name', self.csv_path))
        print(self._just_str('Total Rows with Errors', len(self.rows_w_errors)))
        print(self._just_str('Total Distinct Columns with Errors', len(self.distinct_cols_w_errors)))
        print(line)

    def print_errors(self):
        """
        Print errors collected from Valiation/GeneralSchema objects to console
        """
        # vars for print formatting
        df_length = 100
        header_line = '='*df_length
        line = '-'*df_length
        print('\n' + header_line)

        # create a dataframe of errors, with no duplicate rows
        err_df = pd.DataFrame(columns = self.schema_cols)
        cols = set()
        rows = []
        # individual rows with errors by csv
        for err in self.errors:
            row = err[2]
            cols.add(err[1])
            if row in rows:
                pass
            else:
                rows.append(row)
                err_df = pd.concat([err_df, err[5]])
        # print summary info about csv and error df
        if len(err_df) > 0:
            self._csv_summary_info(rows, cols, line)
            err_df = err_df.sort_index()
            print(err_df, '\n\n')

    def get_df(self):
        return self.df

    def get_distinct_cols_w_errors(self):
        return self.distinct_cols_w_errors

    def get_rows_w_errors(self):
        return self.rows_w_errors


class Combine:
    """
    Given an input list of CSVs, convert the files into 
    a single CSV that matches an expected output schema.
    """

    def __init__(self, insurance_type = None, csvs_path=None):
        s = schema_map(insurance_type)
        self.schema_cols = [v['name'] for _,v in s.get_schema_attrs()]
        self.master_df = pd.DataFrame(columns=self.schema_cols)
        self.insurance_type = insurance_type
        self.csvs_path = csvs_path
        self.total_csvs = 0
        self.total_csvs_w_errors = 0
        self.total_rows_w_errors = 0
        self.distinct_cols_w_errors = set()

    def combine_csvs(self):
        """
        Combine all csvs in indicated path to a single dataframe
        Printing out errors from each CSV as its processed minimizes
            in-memory error cache containing dataframes
        """
        for csv in os.listdir(self.csvs_path):
            # validate/standardize data
            a = AggCSV(self.insurance_type, self.csvs_path + '/' + csv, self.schema_cols)
            a.validate()

            # drop columns from AggCSV df not in defined schema, and add to master df
            drop_columns = [c for c in a.get_df().columns if c not in self.schema_cols]
            temp_df = a.get_df().drop(columns = drop_columns)
            self.master_df.reset_index(drop=True, inplace=True)
            self.master_df = pd.concat([self.master_df, temp_df])

            # print out rows w errors, update totals
            a.print_errors()
            self.total_csvs += 1
            self.total_csvs_w_errors += 1 if len(a.get_rows_w_errors()) > 0 else 0
            self.total_rows_w_errors += len(a.get_rows_w_errors())
            self.distinct_cols_w_errors.update(a.get_distinct_cols_w_errors())
    
    def write_combined_csv(self, output_location='.'):
        """
        Write self.master_df to specified output location.
        Write to current working directory if none is provided.
        """
        date = datetime.today().strftime('%d%m%Y')
        file_name = f'AggIns_combined_csvs_{date}.csv'
        kwargs = {
            'path_or_buf':file_name,
            'index':False,
            'header':True,
            'columns':self.schema_cols
        }
        self.master_df.to_csv(**kwargs)
        output_string = '\nCombined Aggregate Insurance Partner CSVs written to:\n\t' + os.getcwd() + '/' + file_name
        print(output_string)

    def _just_str(self, my_str, val):
        """ justify a number for use in self.print_errors """
        str_len = len(my_str)
        just = 35
        out = my_str  + ' '*(just-str_len) + ': ' + str(val)
        return out

    def print_summary(self):
        """ print stats about errors in all CSVs """
        length = 100
        line = '='*length
        name = 'Aggregator Insurance Inc. 🦓 CSV Error Report'.center(length, ' ')
        divider = '\n'.join(['\n', line, name, line])
        print(divider)
        print(self._just_str('Total Rows with Errors', self.total_rows_w_errors))
        print(self._just_str('Total Combined CSVs', self.total_csvs))
        print(self._just_str('Total Rows in Combined CSV', len(self.master_df)))
        print(self._just_str('Total CSVs with Errors', self.total_csvs_w_errors))
        print(f'Total Distinct Columns with Errors : {len(self.distinct_cols_w_errors)}')
        print(line)
            
def main():
    parser = argparse.ArgumentParser(description='Combine and clean CSV input files from partners', add_help=True)
    parser.add_argument('--csvs_path', type=str, required=True, help='location on system of csv files')
    parser.add_argument('--insurance_type', type=str, default='general', help='only general is available currently')
    args = parser.parse_args()
    itype = args.insurance_type

    c = Combine(itype, args.csvs_path)
    c.combine_csvs()
    c.write_combined_csv()
    c.print_summary()

if __name__ == '__main__':
    main()