from aggregator import schema
from tests.conftest import TestSchema

class SchemaNotFoundError(ValueError):
    """ raise when string is provided with no corresponding schema object"""

def schema_map(schema_type):
    """
    Return schema object from map
    """
    sm = {
        'general':schema.GeneralSchema(),
        'test': TestSchema()
    }
    if schema_type not in sm:
        err_msg = f'Schema obj for string {schema_type} not found'
        raise SchemaNotFoundError(err_msg)

    return sm[schema_type]