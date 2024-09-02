# Creating a pipeline

This creating a pipeline document outlines the steps required to create a pipeline compatible with the data platform. This document will go into detail of steps 1-7 (all steps) in the general pipeline document. It will outline how to achieve a data spanning several years across multiple LAs as highlighted in step 7, but will not include specific/bespoke analyses.

## 1. Create a new pipeline folder with the following subfolders (example files have also been included)

```
liia_tools/
├─ new_pipeline/
│  ├─ __init__.py
│  ├─ stream_filters.py (only for xml)
│  ├─ stream_pipeline.py
│  ├─ stream_record.py (only for xml)
│  ├─ spec/
│  │  ├─ __init__.py
│  │  ├─ pipeline.json
│  │  ├─ new_data_schema_2024.yml or new_data_schema_2024.xsd
│  │  ├─ new_data_schema_2025.diff.yml or new_data_schema_2025.xsd
│  │  ├─ samples/
│  │  │  ├─ __init__.py
│  │  │  ├─ new_data_sample.csv
```

## 2. Create the schemas: use a .yml schema for .csv and .xlsx files, use an .xsd schema for .xml files 

The first .yml schema will be a complete schema for the easliest year of data collection. Afterwards you can create .yml.diff schemas which just contain the differences in a given year and will be applied to the initial .yml schema. \
For .xml files there is no equivalent .xsd.diff so each year will need a complete schema.

* The .yml schema should follow this pattern:

```yaml
column_map:
    file_name:
        header_name:
            cell_type: "property"
            canbeblank: yes/no
        header_name:
            ...
```

* Details of the cell_type and "property" can be found in the Column class in the [__data_schema.py](/liiatools/common/spec/__data_schema.py) file and below are details of how to apply the available types:

```yaml
string: "alphanumeric"

string: "postcode"

string: "regex"

numeric:
    type: "integer"
    min_value: 0 (optional)
    max_value: 10 (optional)

numeric:
    type: "float"
    decimal_places: 2 (optional)

date: "%d/%m/%Y"

category:
    - code: b) Female
      name: F (optional)
      cell_regex: (optional)
      - /.*fem.*/i
      - /b\).*/i

header_regex:
  - /.*child.*id.*/i
```

* Details of how the .diff.yml files are applied can be found in the load_schema function of the [spec/_\_init__.py](/liiatools/ssda903_pipeline/spec/__init__.py) file. The .diff.yml schema should follow this pattern:

```yaml
column_map.header.uniquepropertyreferencenumber:
    type: add
    description: Adds new column for header
    value:
        numeric:
            type: "integer"
            min_value: 1
            max_value: 999999999999
        canbeblank: yes

column_map.social_worker:
  type: add
  description: Adds new table for the 2024 return
  value:
    CHILD:
      string: "alphanumeric"
      canbeblank: no

column_map.episodes.REC:
    type: modify
    description: Updated REC codes for 2019 schema
    value:
        category:
        - code: "X1"
        - code: "E11"
        - code: "E12"

column_map.uasc.sex:
    type: rename
    description: Rename column in uasc
    value:
        gender

column_map.prev_perm:
    type: remove
    description: Remove column from prev_perm
    value:
      - DOB
```

* The .xsd schema should follow the standard xml schema patterns as described in this [XML Schema Tutorial](https://www.w3schools.com/xml/schema_intro.asp). The cleaning performed on the .xsd shchema mimics the .yml schema e.g. postcode, integer, float etc. Details of how the different cleaning functions are implemented can be found in the add_column_spec function in the [cin_pipeline/stream_filters.py](/liiatools/cin_census_pipeline/stream_filters.py) file and below are details of how to apply the available cleaning functions:

```xml
Category
<xs:simpleType name="gendertype">
    <xs:enumeration value="1">
        <xs:annotation><xs:documentation>Male</xs:documentation></xs:annotation> (optional)
    </xs:enumeration>
    <xs:enumeration value="2">
        <xs:annotation><xs:documentation>Female</xs:documentation></xs:annotation>(optional)
    </xs:enumeration>
</xs:simpleType>

Numeric (integer)
<xs:simpleType name="positiveinteger">
    <xs:restriction base="xs:integer">
        <xs:minInclusive value="0"/> (optional)
    </xs:restriction>
</xs:simpleType>

Numeric (float)
<xs:simpleType name="twodecimalplaces">
    <xs:restriction base="xs:decimal">
        <xs:fractionDigits fixed="true" value="2"/> (optional)
        <xs:minInclusive value="0"/> (optional)
        <xs:maxInclusive value="1"/> (optional)
    </xs:restriction>
</xs:simpleType>

Regex
<xs:simpleType name="upn">
    <xs:restriction base="xs:string">
        <xs:pattern value="[A-Za-z]\d{11}(\d|[A-Za-z])"/>
    </xs:restriction>
</xs:simpleType>

Date
<xs:element name="PersonBirthDate" type="xs:date" minOccurs="0" maxOccurs="1"/>

String
<xs:element name="LAchildID" type="xs:string" minOccurs="1" maxOccurs="1"/>
```

## 3. Create the pipeline.json file, these follow the same pattern across all pipelines

* The .json schema should follow this pattern:

```json
{
    "retention_period": {
        "USE_CASE_1": 12,
        "USE_CASE_2": 7,
    },
    "la_signed": {
        "LA_name": {
            "USE_CASE_1": "Yes",
            "USE_CASE_2": "No"
        }
    }
    "table_list": [
        {
        "id": "table_name",
        "retain": [ 
            "USE_CASE_TO_RETAIN_FOR" (optional)
        ]
        "columns": [
            {
            "id": "column_1",
            "type": "type",
            "unique_key": true, (optional)
            "enrich": [
                "enrich_function_1", (optional)
                "enrich_function_2" (optional)
            ]
            "degrade": "degrade_function" (optional)
            "sort": sort_order_integer (optional)
            "exclude": [
                "USE_CASE_TO_EXCLUDE_FROM" (optional)
            ]
            }
        ]
        }
    ]
}
```
* Details of the different available values can be found in the PipelineConfig class in the [__config.py](/liiatools/common/data/__config.py) file.
* Details of the different possible enrich and degrade functions can be found in the [_transform_functions.py](/liiatools/common/_transform_functions.py) file.

## 4. Create the new_pipeline/spec/_\_init__.py file which will load the schema and the pipeline

* For .csv or .xlsx files use the [ssda903_pipeline/spec/_\_init__.py](/liiatools/ssda903_pipeline/spec/__init__.py) as a template. The only changes needed are two SSDA903 references in the load_schema function, these should be renamed to reflect the new use case name and therefore schema names.

* For .xml files use the [cin_census_pipeline/spec/_\_init__.py](/liiatools/cin_census_pipeline/spec/__init__.py) as a template. Similarly the only changes needed are the two CIN references in the load_schema function, these should be renamed to reflect the new use case name and therefore schema names.

## 5. Create the stream_pipeline.py file. These will vary slighly depending on the type of data you wish to produce a pipeline for

* For .csv files use the [ssda903_pipeline/stream_pipeline.py](/liiatools/ssda903_pipeline/stream_pipeline.py) file. You can simply copy the file and update the docstrings to refer to the new dataset.

* For .xlsx files use the [annex_a_pipeline/stream_pipeline.py](/liiatools/annex_a_pipeline/stream_pipeline.py) file. Again you can copy this file and update the docstrings to refer to the new dataset. For these file types you will also need to copy the [annex_a_pipeline/stream_filters.py](/liiatools/annex_a_pipeline/stream_filters.py) to the same location as the stream_pipeline.py file.

* For .xml files use the [cin_pipeline/stream_pipeline.py](/liiatools/cin_pipeline/stream_pipeline.py) file. Again you can copy this file and update the docstrings to refer to the new dataset. For these file types you will also need to copy the [cin_pipeline/stream_filters.py](/liiatools/cin_pipeline/stream_filters.py) and [cin_pipeline/stream_record.py](/liiatools/cin_pipeline/stream_record.py) files. It will be easiest to copy over these file initially and make small adjustments as needed. These adjustments are described in detail below.

## 6. Create stream_filters.py and stream_record.py files for .xml pipelines. These files will both vary slighly depending on the data within the pipeline

* The stream_filters.py file will need changes in the add_column_spec function. This is where we convert the values in the .xsd schema to mimic the values in a .yml schema, allowing the cleaning of the pipelines to function in the same way. Examples of changes are:

```python
if config_type is not None:
    if config_type[-4:] == "type":  # no change needed, looks in the .xsd for categories which should all end with "type"
        column_spec.category = _create_category_spec(config_type, schema_path)
    if config_type in ["positiveinteger"]:  # include all nodes that should be turned into numeric values
        column_spec.numeric = _create_numeric_spec(config_type, schema_path)
    if config_type in ["upn"]:  # include all nodes that should be cleaned with regex values
        column_spec.string = "regex"
        column_spec.cell_regex = _create_regex_spec(config_type, schema_path)
    if config_type == "{http://www.w3.org/2001/XMLSchema}date":  # no change needed, looks in the .xsd for dates
        column_spec.date = "%Y-%m-%d"
    if config_type == "{http://www.w3.org/2001/XMLSchema}dateTime":  # no change needed, looks in the .xsd for datetimes
        column_spec.date = "%Y-%m-%dT%H:%M:%S"
    if config_type in [  # keep these values but may need to add other default integer values
        "{http://www.w3.org/2001/XMLSchema}integer",
        "{http://www.w3.org/2001/XMLSchema}gYear",
    ]:
        column_spec.numeric = Numeric(type="integer")
    if config_type == "{http://www.w3.org/2001/XMLSchema}string":  # no change needed, looks in the .xsd for strings
        column_spec.string = "alphanumeric"
```

* The stream_record.py will need changes to the classes and collectors. The current cin_pipeline/stream_record.py is specific to the cin census use case and should not be applied in general terms. This module tries to create one .csv of cin events, in general we want to create one .csv file per .xml node with just one layer of subnodes (no xs:complexType within an xs:complexType) e.g. for Children's Social Work Workforce we have:

```xml
<xs:complexType name="messagetype">
    <xs:sequence>
      <xs:element name="Header" type="headertype" minOccurs="0" maxOccurs="1"/>
      <xs:element name="LALevelVacancies" type="vacancytype" minOccurs="1" maxOccurs="1"/>
	  <xs:element name="CSWWWorker" type="workertype" minOccurs="1" maxOccurs="unbounded"/>
    </xs:sequence>
  </xs:complexType>
```
* Here Header, LALevelVacancies and CSWWWorker just contain one layer of subnodes each, so we create a .csv for each of these (although Header doesn't actually contain any useful information so we ignore that).

* Changes that are needed for the stream_record.py file include:

```python
Classes
class CSWWEvent(events.ParseEvent):  # rename this class to the align with the xs:element you are interested in, e.g. CSWWWorker -> CSWWEvent
    @staticmethod
    def name():
        return "worker"  # rename this to a sensible name that will be used to name the .csv file

    pass

Collector
def message_collector(stream):
    """
    Collect messages from XML elements and yield events

    :param stream: An iterator of events from an XML parser
    :yield: Events of type HeaderEvent, CSWWEvent or LALevelEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", f"Expected Message, got {stream.peek().tag}"
    while stream:
        event = stream.peek()
        if event.get("tag") == "Header":  # Rename this to match the .xsd schema
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)  # Rename this to match the class you have created  
        elif event.get("tag") == "CSWWWorker":  # Rename this to match the .xsd schema
            csww_record = text_collector(stream)
            if csww_record:
                yield CSWWEvent(record=csww_record)  # Rename this to match the class you have created  
        elif event.get("tag") == "LALevelVacancies":  # Rename this to match the .xsd schema
            lalevel_record = text_collector(stream)
            if lalevel_record:
                yield LALevelEvent(record=lalevel_record)  # Rename this to match the class you have created  
        else:
            next(stream)

Export
@generator_with_value
def export_table(stream):  # This function should stay the same but is not currently used for the cin census so I have created a version here
    """
    Collects all the records into a dictionary of lists of rows

    This filter requires that the stream has been processed by `message_collector` first

    :param stream: An iterator of events from message_collector
    :yield: All events
    :return: A dictionary of lists of rows, keyed by record name
    """
    dataset = {}
    for event in stream:
        event_type = type(event)
        dataset.setdefault(event_type.name(), []).append(event.as_dict()["record"])
        yield event
    return dataset
```
## 7. Adjust the [assets/common.py](/liiatools_pipeline/assets/common.py) file to include the new pipeline.json file you have created

* You just need to add an import like so:

```python
from liiatools.new_pipeline.spec import load_pipeline_config as load_pipeline_config_dataset
```

## 8. Adjust the [ops/common_la.py](/liiatools_pipeline/ops/common_la.py) file to include the new schema and task_cleanfile you have created

* You just need to add an import like so:

```python
from liiatools.new_pipeline.spec import load_schema as load_schema_dataset
from liiatools.new_pipeline.stream_pipeline import task_cleanfile as task_cleanfile_dataset
```