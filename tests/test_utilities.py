import pytest

from aggregator.utilities import schema_map, SchemaNotFoundError
from aggregator.schema import GeneralSchema

def test_schema_map_instantiate():
    """ test correct schema object is instantiated """
    itypes = {'general':GeneralSchema}
    for itype in itypes:
        s = schema_map(itype)
        assert itypes[itype] == type(s)

def test_schema_map_raise_exception():
    """ test exception is raised when non-present class specified """
    with pytest.raises(SchemaNotFoundError, match=r'Schema obj for string not_found not found'):
        schema_map('not_found')
