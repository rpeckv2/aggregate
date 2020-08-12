import inspect
import string
import functools

import validators
import numpy as np

class SchemaTypeNotDeclared(ValueError):
    """ raise this when a type is not declared for a schema class """

class Validation:
    """
    All insurance classes inherit this class of methods
        for validating data from partners and tracking errors.
    """
    def __init__(self):
        self.row = 0
        self.rows_w_errors = set()
        self.distinct_cols_w_errors = set()
        self.errors = []
        self.std_validations = [self.validate_type, self.validate_null]

    def _error_count(func):
        """
        Provided to decorate Validation methods to allow
            tracking of errors as data is being validated/cleaned
        """
        @functools.wraps(func)
        def error_tracker(self, *args):
            x = args[0]
            col = args[1]
            csv_path = args[2]
            df_row = args[3]
            result = func(self, x, col)
            
            if not result:
                df_row = df_row.loc[self.row].to_frame().T
                self.rows_w_errors.add(self.row)
                self.distinct_cols_w_errors.add(col['name'])
                self.errors.append([csv_path, col['name'], self.row, func.__name__, x, df_row])

            self.row += 1
            return result

        return error_tracker

    def get_schema_attrs(self):
        """
        Return defined schema class attributes and values as map
        """
        for n, d in inspect.getmembers(self):
            if (type(d) == dict
                and '__' not in n):
                yield n, d

    def set_row(self, val):
        self.row = val
    
    def get_rows_w_errors(self):
        return self.rows_w_errors

    def get_distinct_cols_w_errors(self):
        return self.distinct_cols_w_errors

    def get_errors(self):
        return self.errors

    def get_std_validations(self):
        return self.std_validations

    # ---------------------------------------------------
    # standard column validation
    # ---------------------------------------------------
    @_error_count
    def validate_type(self, *args):
        """
        Try to convert datum to expected schema type
        """
        x, col = args[0], args[1]
        if not col.get('type'):
            err_msg = 'No type declared for {}'.format(col['name'])
            raise SchemaTypeNotDeclared(err_msg)
        else:
            try:
                # if converting to a number strip out any non-numerical chars
                if col['type'] != str:
                    x = ''.join([i for i in str(x) if i in string.digits or i == '.'])
                x = col['type'](x)
                return x
            except:
                return False

    @_error_count
    def validate_null(self, *args):
        """
        If only characters in datum are whitespace
            or there is no data
            or a form of N/A is present
            or a form of NULL is present
            or '0' is present (ad click val should likely not be 0)
            then the value can be deemed null
        """
        x, col = args[0], args[1]
        y = str(x).lower().replace('/', '')
        null_vals = ['null', 'na', '0', 'nan']
        ws_chars = sum([1 for c in y if c in string.whitespace])
        null_datum = False
        if (ws_chars == len(y)
            or len(y) == 0
            or y in null_vals):
            null_datum = True

        if null_datum and not col['nullable']:
            return False
        elif null_datum:
            return np.nan
        else:
            return x
            
    # ---------------------------------------------------
    # specific column validation
    # ---------------------------------------------------
    # _error_count
    # def valid_provider_name(self, *args):
        # """ removing non-alphanumeric characters and
        # lowercasing the provider name is a good idea to standardize
        # naming for grouping/aggregation later. At this time altering
        # the names may prove confusing however
        # """

    @_error_count
    def upper_campaign_id(self, *args):
        """ campaign_ids in csv files appear to be all uppercased
        , standardizing the case can ensure correct groupings """
        x = str(args[0])
        return x.upper()

    @_error_count
    def valid_cost_per_ad_click(self, *args):
        """
        Cost per ad click cannot be zero
        """
        x = float(args[0])
        if x != 0:
            return x
        return False

    @_error_count
    def valid_redirect_link(self, *args):
        """
        """
        x = str(args[0])
        my_domain = x.split('/',1)[0]
        if validators.domain(my_domain):
            return x
        return False

    @_error_count
    def valid_phone_number(self, *args):
        """
        Check if format of phone number is correct, does not verify if
            this is a working number.
        Standardize all valid phone numbers to be of form (xxx)xxx-xxxx
            or xxx-xxxx.
        """
        x = str(args[0])
        x = ''.join([i for i in x if i in string.digits])
        if len(x) == 7 or len(x) == 10:
            if len(x) == 7:
                x = f'{x[:3]}-{x[3:]}'
            else:
                x = f'({x[:3]}){x[3:6]}-{x[6:]}'
            return x
        else:
            return False

    @_error_count
    def standardize_address(self, *args):
        """ standardize address, validating if its a legitimate
        address not currently supported """
        x = args[0]
        return string.capwords(str(x))

    @_error_count
    def valid_zipcode(self, *args):
        """
        Only checks if zipcode is correct format.
        Does not check if zipcode is legitimate.
        Standardize zipcodes to xxxxx-xxxx and xxxxx
        """
        x = str(args[0])
        x = ''.join([i for i in x if i in string.digits])
        if len(x) == 5 or len(x) == 9:
            if len(x) == 9:
                x = f'{x[:5]}-{x[5:]}'
            return x
        else:
            return False

class GeneralSchema(Validation):
    """
    Schema for Insurance CSVs defined as code.

    Attributes are maps containing details about column
        types and needed validation methods.

    Type and nullability are checked for all columns
        by default.
    """
    provider_name = {
        'name':'Provider Name',
        'type':str,
        'nullable':False,
        'validations':[]
    }
    campaign_id = {
        'name':'CampaignID',
        'type':str,
        'nullable':False,
        'validations':[Validation.upper_campaign_id]
    }
    cost_per_ad_click = {
        'name':'Cost Per Ad Click',
        'type':float,
        'nullable':False,
        'validations':[Validation.valid_cost_per_ad_click]
    }
    redirect_link = {
        'name':'Redirect Link',
        'type':str,
        'nullable':False,
        'validations':[Validation.valid_redirect_link]
    }
    phone_number = {
        'name':'Phone Number',
        'type':str,
        'nullable':True,
        'validations':[Validation.valid_phone_number]
    }
    address = {
        'name':'Address',
        'type':str,
        'nullable':False,
        'validations':[Validation.standardize_address]
    }
    zipcode = {
        'name':'Zipcode',
        'type':str,
        'nullable':False,
        'validations':[Validation.valid_zipcode]
    }





    