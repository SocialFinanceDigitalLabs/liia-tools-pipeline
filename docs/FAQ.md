# Frequently Asked Questions

## 1. How do I add a new cleaning function?

* The cleaning functions themselves should be added to the [common/converters.py](/liiatools/common/converters.py) file. These are built to accept individual values and return a clean value. If there are any errors we want to raise ValueError with an appropriate error message.

* The new cleaning function can then be implemented in the conform_cell_types function in the [common/stream_filters.py](/liiatools/common/stream_filters.py) file. Here we want to add an new: `if column_spec.type == "new_type"` and the corresponding new cleaning function.

* The new `column_spec.type` needs to align with the Column class in the [__data_schema.py](/liiatools/common/spec/__data_schema.py) file. Here is where you will want to add the new type will will follow the pattern `new_type_name: possible_types`. The `possible_types` can range from simple string literals to more complex classes, such as the Numeric class or Category class.

* Once complete be sure to add unit tests for the cleaning functions to the [common/test_converters.py](/liiatools/tests/common/test_converters.py) and [common/test_filter.py](/liiatools/tests/common/test_filter.py) files.

## 2. How do I apply a new cleaning function to .xml files?

* Once you have followed the steps outlined in question 1, you may need to create a new function that converts the .xsd schema into a Column class that is readable by the pipeline. Examples of these can be found in the [common/stream_filters.py](/liiatools/common/stream_filters.py) file. How you apply these will depend on the naming conventions you have used in the .xsd schema but looking at the examples should give you an idea. Below please find a more detailed breakdown of the existing _create_category_spec function:

```python
def _create_category_spec(field: str, file: Path) -> List[Category] | None:
    """
    Create a list of Category classes containing the different categorical values of a given field to conform categories
    e.g. [Category(code='0', name='Not an Agency Worker'), Category(code='1', name='Agency Worker')]

    :param field: Name of the categorical field you want to find the values for
    :param file: Path to the .xsd schema containing possible categories
    :return: List of Category classes of categorical values and potential alternatives
    """
    category_spec = []

    xsd_xml = ET.parse(file)  # Use the xml.etree.ElementTree parse functionality to read the .xsd schema
    search_elem = f".//{{http://www.w3.org/2001/XMLSchema}}simpleType[@name='{field}']"  # Use the field argument to search for a specific field, this will be a string you have determined in the stream_filters.py file e.g. "some_string_ending_in_type". You can see the simpleType aligns with simpleType in the .xsd schema
    element = xsd_xml.find(search_elem)  # Search through the .xsd schema for this field

    if element is not None:
        search_value = f".//{{http://www.w3.org/2001/XMLSchema}}enumeration"  # Find the 'code' parameter which is within the .xsd enumeration node
        value = element.findall(search_value)
        if value:
            for v in value:  # The category element of the Column class is a list of Category classes, so we append a Category class to a list
                category_spec.append(Category(code=v.get("value")))  # Grab the value found in the .xsd schema

            search_doc = f".//{{http://www.w3.org/2001/XMLSchema}}documentation"  # Find the 'name' parameter which is within the .xsd documentation node
            documentation = element.findall(search_doc)
            for i, d in enumerate(documentation):  # Use enumerate to correctly loop through the existing Category classes
                category_spec[i].name = d.text  # Add a name value to the existing Category classes so each one has both code and name

                #  Before returning the finished category_spec you could for example add another loop to add potential regex patterns if necessary
            return category_spec
    else:
        return
```

## 3. How do I add new enriching / degrading functions?

* The new enriching and degrading functions should be added to the [_transform_functions.py](/liiatools/common/_transform_functions.py) file. These are built to accept a row (pd.Series) of data and output transformed values. These functions can vary from being simple additions of metadata, such as the year or LA, to specific hashing values of a specific column.

* You will need to add the new function(s) and then include this in the corresponding enrich_functions/degrade_functions dictionaries. The key in this dictionaries determine what key to put in the corresponding pipeline.json file, and the value determines what function is performed. e.g. `"year": add_year` will be called in the pipeline.json like this:

```json
"id": "YEAR",
"type": "integer",
"enrich": "add_year",
"sort": 0
```

* Here we have created a new column called YEAR, of type integer, using the enrich function `add_year` and finally being the first column when it comes to sort order. 


