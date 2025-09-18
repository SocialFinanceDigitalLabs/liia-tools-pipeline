# Import statements until line 17 below

import logging
from pathlib import Path

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import pass_event, streamfilter

from liiatools.common.spec.__data_schema import Column, Numeric
from liiatools.common.stream_filters import (
    _create_category_spec,
    _create_numeric_spec,
    _create_regex_spec,
)

logger = logging.getLogger(__name__)

# streamfilter decorator tells function how to run based on another function
# streamfilter decorator is one SF has put together
@streamfilter(
    check=type_check(events.TextNode),
    fail_function=pass_event,
    error_function=pass_event,
)

# Define our function
# schema_path is of the type Path
def add_column_spec(event, schema_path: Path):
# Keep in mind indentations (don't indent more or less than you should be)
# doc strings are made using 3 double quotes - everything in there is a string
    """
    Add a Column class containing schema attributes to an event object based on its type and occurrence

    :param event: An event object with a schema attribute
    :param schema_path: The path to the schema file
    :return: A new event object with a column_spec attribute, or the original event object if no schema is found
    """
    # The capital 'c' in 'Column' means Column is a class
    # Classes are like functions but code isn't run - e.g. a dataframe of 903
    # It's a custom class 
    column_spec = Column()
    # Things ending 'dantic' e.g. pydantic, means being pedantic (to do with classes)
    # if you have a class in a class, the first one inherits 2nd class' qualities
    # BaseModel is used to influence Column; BaseModel function we will fill in with what we want

    # If 1st element is 1, i.e. 1st column comes up, then it can't be blank
    # Error will come up if it's blank
    # If it's not 1 then this bit will be blank, and will proceed to next bit
    if event.schema.occurs[0] == 1:
        column_spec.canbeblank = False
    
    # . bits is a subpriority of previous thing
    config_type = event.schema.type.name
    # Now we're making a big if statement
    # Look back at XSD schema and put in each possibility
    # Each type in XSD must be here e.g. twodecimalplaces is in CSWW but not CIN so we must add here
    if config_type is not None:
        if config_type[-4:] == "type":
            column_spec.category = _create_category_spec(config_type, schema_path)
    # Ignore 4 lines below and replace with 2 at the end
    #       if config_type == "onedecimalplace":
    #            column_spec.numeric = _create_numeric_spec(config_type, schema_path)
    #       if config_type == "twodecimalplaces":
    #            column_spec.numeric = _create_numeric_spec(config_type, schema_path)
    #       if "decimalplace" in config_type:
    #            column_spec.numeric = _create_numeric_spec(config_type, schema_path)
    # Changed from decimalplace to decimal to account for new ftedecimal (renamed ftetype in xsd schemas)        
            if "decimal" in config_type:
                column_spec.numeric = _create_numeric_spec(config_type, schema_path)
                # All 3 of us changed ftetype instances to ftedecimal in csww_schema_2025.xsd and previous years' too
                # This will make doing below easier
                # if upnnumber was also a regex for instance then change below to ["swetype", "upnnumber"]
            if config_type in ["swetype"]:
                column_spec.string = "regex":
                column_spec.cell_regex = _create_regex_spec(config_type, schema_path)
            
            if config_type == "{http://www.w3.org/2001/XMLSchema}date":
                column_spec.date = "%Y-%m-%d"
            if config_type == "{http://www.w3.org/2001/XMLSchema}dateTime":
                column_spec.date = "%Y-%m-%dT%H:%M:%S"
            if config_type in [
                "{http://www.w3.org/2001/XMLSchema}integer",
                "{http://www.w3.org/2001/XMLSchema}gYear",
            ]:
                column_spec.numeric = Numeric(type="integer")
            if config_type == "{http://www.w3.org/2001/XMLSchema}string":
                column_spec.string = "alphanumeric"
    else:
        column_spec.string = "alphanumeric"

    return event.from_event(event, column_spec=column_spec)