from typing import Iterator, Optional
 
from more_itertools import peekable
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import generator_with_value
 
from liiatools.common.stream_record import HeaderEvent, _reduce_dict, text_collector
 
# class SEN2Event(events.ParseEvent):
#     @staticmethod
#     def name():
#         return "sen2"
 
#     pass
 
 
class SourceEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Source"
 
    pass
 
 
class PersonEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Person"
 
    pass
 
 
class RequestsEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Requests"
 
    pass
 
class AssessmentEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Assessment"
 
    pass
 
class NamedPlanEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "NamedPlan"
 
    pass
 
class PlanDetailEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "PlanDetail"
 
    pass
 
class ActivePlansEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "ActivePlans"
 
    pass
 
class PlacementDetailEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "PlacementDetail"
 
    pass
 
class SENneedEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "SENneed"
 
    pass
 
 
# Collects text under certain xml tags and builds a dictionary of results.
# While loop: look at th enext xml event/node without moving forward. If the
# event has a tag property, save it in last_tag; if not, keep the previous value.
#if the tag is ine one of the nodes of interest, call text_collector function (consumes the node and extracts all text under it)
# and stores this in the data_dict as a list.
# if the current tag is not one of the relevant ones, append to list
# issue: not hierarchical; flattens everything. Is this a problem?
 
@xml_collector
def sen2_collector(stream):
    """
    Create a dictionary of text values for each SEN2 element;
    Requests, Assessment, NamedPlan, PlanDetail, ActivePlans, PlacementDetail, SENneed
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    print(f">>>sen2/namedplan_collector: called with {stream.peek().tag=}")
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        if event.get("tag") in (
            "PlanDetail",
            "PlacementDetail",
            "SENneed",
        ):
            print(f">>>sen2/sen2_collector: Next tag is {event.get("tag")}, calling text_collector...")
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
            print(f">>>sen2/sen2_collector: Created {data_dict=}")
        else:
            if isinstance(event, events.TextNode) and event.cell:
                print(f">>>sen2/sen2_collector: adding {last_tag}:{event.cell} to data_dict...")
                data_dict.setdefault(last_tag, []).append(event.cell)
            next(stream)
    print(">>>sen2/se2_collector: Finished stream - returning record in data_dict...")

    return _reduce_dict(data_dict)
 
 
@xml_collector
def person_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "Person"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        print(f">>>sen2/person_collector: set {last_tag=}")
        print(f">>>sen2/person_collector:  {type(event)=}")
        if event.get("tag") in ("Requests",):
            #print(">>>sen2/person_collector: Collecting **ALL** items at Person level using text_collector- WRONG - collects data at lower levels too!")
            print(">>>sen2/requests_collector: Collecting Requests level")
            data_dict.setdefault(event.tag, []).append(requests_collector(stream))
        else:
            print(f">>>sen2/person_collector: Got <{last_tag}> tag, check if it is a TextNode with value...")
            if isinstance(event, events.TextNode) and event.cell:
                print(f">>>sen2/person_collector: Got <{last_tag}> with {event.cell=}, appending directly to data_dict...")
                data_dict.setdefault(last_tag, []).append(event.cell)
                print(f">>>sen2/person_collector: {data_dict=}")
            else:
                print(">>>sen2/person_collector: Not a TextNode, next stream..")
            next(stream)
 
    return _reduce_dict(data_dict)
 
@xml_collector
def requests_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "Requests"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        print(f">>>sen2/requests_collector: set {last_tag=}")
        print(f">>>sen2/requests_collector:  {type(event)=}")
        if event.get("tag") in ("Assessment",):
            print(">>>sen2/requests_collector: Calling assessment_collector...")
            data_dict.setdefault(event.tag, []).append(assessment_collector(stream))
        elif event.get("tag") in ("ActivePlans",):
            print(">>>sen2/requests_collector: Calling activeplans_collector...")
            data_dict.setdefault(event.tag, []).append(activeplans_collector(stream))
        else:
            print(f">>>sen2/requests_collector: Got <{last_tag}> tag, check if it is a TextNode with value...")
            if isinstance(event, events.TextNode) and event.cell:
                print(f">>>sen2/requests_collector: Got <{last_tag}> with {event.cell=}, appending directly to data_dict...")
                data_dict.setdefault(last_tag, []).append(event.cell)
                print(f">>>sen2/requests_collector: {data_dict=}")
            else:
                print(">>>sen2/requests_collector: Not a TextNode, next stream..")
            next(stream)
 
    return _reduce_dict(data_dict)

@xml_collector
def assessment_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "Assessment"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        print(f">>>sen2/assessment_collector: set {last_tag=}")
        print(f">>>sen2/assessment_collector:  {type(event)=}")
        if event.get("tag") in ("NamedPlan",):
            print(">>>sen2/assessment_collector: Calling namedplan_collector...")
            data_dict.setdefault(event.tag, []).append(namedplan_collector(stream))
        else:
            print(f">>>sen2/assessment_collector: Got <{last_tag}> tag, check if it is a TextNode with value...")
            if isinstance(event, events.TextNode) and event.cell:
                print(f">>>sen2/assessment_collector: Got <{last_tag}> with {event.cell=}, appending directly to data_dict...")
                data_dict.setdefault(last_tag, []).append(event.cell)
                print(f">>>sen2/assessment_collector: {data_dict=}")
            else:
                print(">>>sen2/assessment_collector: Not a TextNode, next stream..")
            next(stream)
 
    return _reduce_dict(data_dict)


@xml_collector
def namedplan_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "NamedPlan"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        print(f">>>sen2/namedplan_collector: set {last_tag=}")
        print(f">>>sen2/namedplan_collector:  {type(event)=}")
        if event.get("tag") in (
                "PlanDetail",
            ):
                print(f">>>sen2/namedplan_collector: Next tag is {event.get("tag")}, calling text_collector...")
                data_dict.setdefault(event.tag, []).append(text_collector(stream))
                print(f">>>sen2/namedplan_collector: Created {data_dict=}")
        else:
            print(f">>>sen2/namedplan_collector: Got <{last_tag}> tag, check if it is a TextNode with value...")
            if isinstance(event, events.TextNode) and event.cell:
                print(f">>>sen2/namedplan_collector: Got <{last_tag}> with {event.cell=}, appending directly to data_dict...")
                data_dict.setdefault(last_tag, []).append(event.cell)
                print(f">>>sen2/namedplan_collector: {data_dict=}")
            else:
                print(">>>sen2/namedplan_collector: Not a TextNode, next stream..")
            next(stream)
 
    return _reduce_dict(data_dict)

@xml_collector
def activeplans_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "ActivePlans"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        print(f">>>sen2/activeplans_collector: set {last_tag=}")
        print(f">>>sen2/activeplans_collector:  {type(event)=}")
        if event.get("tag") in (
                "PlacementDetail",
                "SENneed",
            ):
                print(f">>>sen2/activeplans_collector: Next tag is {event.get("tag")}, calling text_collector...")
                data_dict.setdefault(event.tag, []).append(text_collector(stream))
                print(f">>>sen2/activeplans_collector: Created {data_dict=}")
        else:
            print(f">>>sen2/activeplans_collector: Got <{last_tag}> tag, check if it is a TextNode with value...")
            if isinstance(event, events.TextNode) and event.cell:
                print(f">>>sen2/activeplans_collector: Got <{last_tag}> with {event.cell=}, appending directly to data_dict...")
                data_dict.setdefault(last_tag, []).append(event.cell)
                print(f">>>sen2/activeplans_collector: {data_dict=}")
            else:
                print(">>>sen2/activeplans_collector: Not a TextNode, next stream..")
            next(stream)
 
    return _reduce_dict(data_dict)


@xml_collector
def source_collector(stream):
    """
    Create a dictionary of text values for each Child element; ChildIdentifiers, ChildCharacteristics and CINdetails
 
    :param stream: An iterator of events from an XML parser
    :return: Dictionary containing element name and text values
    """
    data_dict = {}
    stream = peekable(stream)
    print(f">>>sen2/source_collector: called with {stream.peek().tag=}")
    assert stream.peek().tag == "Source"
    while stream:
        event = stream.peek()
        if event.get("tag") in ("Source"):
            print(">>>sen2/source_collector: appending items from stream to data_dict using text_collector...")
            data_dict.setdefault(event.tag, []).append(text_collector(stream)) # temporarily hardcoded until we fix json, should be 'event.tag'
        # elif event.get("tag") == "Person":
        #     data_dict.setdefault(event.tag, []).append(person_collector(stream))
        else:
            next(stream)
    print(f">>>sen2/source_collector: Finished - returning record...")
    return _reduce_dict(data_dict)
 
 
def message_collector(stream):
    """
    Collect messages from XML elements and yield events
 
    :param stream: An iterator of events from an XML parser
    :yield: Events of type SourceEvent, PersonEvent, RequestsEvent, AssessmentEvent, NamedPlanEvent, PlanDetailEvent, ActivePlansEvent, PlacementDetailEvent, or SENneedEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", f"Expected Message, got {stream.peek().tag}"
    while stream:
        event = stream.peek()
        print(f">>>sen2/message_collector: Peek - next event = {type(event)} {event.get("tag")}")
        if event.get("tag") == "Source":
            print(">>>sen2/message_collector: Got <Source> tag, calling source_collector...")
            header_record = source_collector(stream)
            if header_record:
                print(f">>>sen2/message_collector: source_collector returned {header_record=} (yielding...)")
                yield SourceEvent(record=header_record)
        elif event.get("tag") == "Person":
            print(">>>sen2/message_collector: Got <Person> tag, calling person_collector...")
            person_record = person_collector(stream)
            if person_record:
                print(f">>>sen2/message_collector: person_collector returned {person_record=} (yielding...)")
                yield PersonEvent(record=person_record)
        else:
            next(stream)
 
 
def _maybe_list(value):
    """
    Ensures that the given value is a list.
 
    Parameters:
    - value: The value to be converted to a list. It can be of any type.
 
    Returns:
    - A list containing the original value(s).
 
    Behavior:
    - If the input value is None, the function returns an empty list.
    - If the input value is already a list, it is returned as is.
    - For any other value, the function wraps it in a list and returns it.
    """
    print(f">>>sen2/_maybe_list: {value=}")
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value
 
def sen2_event(record, export_headers, event_name: Optional[str] = None):
    """
    Create an event record based on the given property from the original record.
 
    This function takes a dictionary `record` and extracts the value of a specified
    `property`. If the property exists and is non-empty, it creates a new dictionary
    with keys "Date" and "Type" where "Date" is the value of the specified property
    and "Type" is the name of the event. The new dictionary is then filtered based
    on the keys specified in export_headers.
 
    Parameters:
    - record (dict): The original record containing various key-value pairs.
    - property (str): The key in the `record` dictionary to look for.
    - event_name (str, optional): The name of the event. Defaults to the value of `property` if not specified.
    - export_headers (list): A list of keys to include in the returned dictionary.
 
    Returns:
    - tuple: A single-element tuple containing the new filtered dictionary, or an empty tuple if `property` is not
    found or its value is empty.
 
    Note:
    - The reason this returns a tuple is that when called, it is used with 'yield from' which expects an iterable.
    An empty tuple results in no records being yielded.
    """
    print(f">>>sen2/sen2_event: {export_headers=}")
    return ({k: record.get(k) for k in export_headers},)
 
 
def event_to_records(event: RequestsEvent, output_columns: list) -> Iterator[dict]:
    """
    Transforms a CINEvent into a series of event records.
 
    The CINEvent has to have a record. This is generated from :func:`child_collector`
 
    Parameters:
    - event (CINEvent): A CINEvent object containing the record and various details related to it.
    - output_columns (list): A list of column names that should be allowed in the output records.
 
    Returns:
    - Iterator[dict]: An iterator that yields dictionaries representing individual event records.
 
    Behavior:
    - The function first creates a 'child' dictionary by merging the "ChildIdentifiers" and "ChildCharacteristics"
    from the original event record.
    - It then processes various sub-records within the event, including "CINdetails", "Assessments", "CINPlanDates",
    "Section47", and "ChildProtectionPlans".
    - Each sub-record is further processed and emitted as an individual event record.
    """
    print(f">>>sen2/event_to_records: called with {event.name()=} and {output_columns=}")
    event_name = type(event).name()
    record = event.record
 
    sen2 = {
        **record.get(event_name, {})
    }
 
    for sen2_item in _maybe_list(record.get(event_name)):
        yield from sen2_event(
                {**sen2, **sen2_item},
                export_headers=output_columns,
        )
 
 
    # person = {
    #     **record.get("Person", {})
    # }
 
    # for person_item in _maybe_list(record.get("Person")):
    #     yield from sen2_event(
    #             {**person, **person_item},
    #             export_headers=output_columns,
    #     )
 
    # requests = {
    #     **record.get("Requests", {})
    # }
 
    # for requests_item in _maybe_list(record.get("Requests")):
    #     yield from sen2_event(
    #             {**requests, **requests_item},
    #             export_headers=output_columns,
    #     )
 
 
    # header = {
    #     **record.get("header", {})
    # }
 
    # for header_item in _maybe_list(record.get("header")):
    #     yield from sen2_event(
    #             {**header, **header_item},
    #             export_headers=output_columns,
    #     )
 
    print(f">>>sen2/event_to_records: {record.keys()=}")
    # child["Disabilities"] = ",".join(_maybe_list(child.get("Disability")))
 
    # for cin_item in _maybe_list(record.get("CINdetails")):
    #     yield from cin_event(
    #         {**child, **cin_item}, "CINreferralDate", export_headers=output_columns
    #     )
    #     yield from cin_event(
    #         {**child, **cin_item}, "CINclosureDate", export_headers=output_columns
    #     )
 
    #     for assessment in _maybe_list(cin_item.get("Assessments")):
    #         assessment["Factors"] = ",".join(
    #             _maybe_list(assessment.get("AssessmentFactors"))
    #         )
    #         yield from cin_event(
    #             {**child, **cin_item, **assessment},
    #             "AssessmentActualStartDate",
    #             export_headers=output_columns,
    #         )
    #         yield from cin_event(
    #             {**child, **cin_item, **assessment},
    #             "AssessmentAuthorisationDate",
    #             export_headers=output_columns,
    #         )
 
    #     for cin in _maybe_list(cin_item.get("CINPlanDates")):
    #         yield from cin_event(
    #             {**child, **cin_item, **cin},
    #             "CINPlanStartDate",
    #             export_headers=output_columns,
    #         )
    #         yield from cin_event(
    #             {**child, **cin_item, **cin},
    #             "CINPlanEndDate",
    #             export_headers=output_columns,
    #         )
 
    #     for s47 in _maybe_list(cin_item.get("Section47")):
    #         yield from cin_event(
    #             {**child, **cin_item, **s47},
    #             "S47ActualStartDate",
    #             export_headers=output_columns,
    #         )
 
    #     for cpp in _maybe_list(cin_item.get("ChildProtectionPlans")):
    #         yield from cin_event(
    #             {**child, **cin_item, **cpp},
    #             "CPPstartDate",
    #             export_headers=output_columns,
    #         )
    #         yield from cin_event(
    #             {**child, **cin_item, **cpp},
    #             "CPPendDate",
    #             export_headers=output_columns,
    #         )
    #         for cpp_review in _maybe_list(cpp.get("CPPreviewDate")):
    #             cpp_review = {"CPPreviewDate": cpp_review}
    #             yield from cin_event(
    #                 {**child, **cin_item, **cpp, **cpp_review},
    #                 "CPPreviewDate",
    #                 export_headers=output_columns,
    #             )
 
 
 
 
 
@generator_with_value
def export_table(stream, output_config):
    """
    Collects all the records into a dictionary of lists of rows
 
    This filter requires that the stream has been processed by `message_collector` first
 
    :param stream: An iterator of events from message_collector
    :param output_config: Configuration for the output, imported as a PipelineConfig class
    :yield: All events
    :return: A dictionary of lists of rows, keyed by record name
    """
    dataset = {}
    print(">>>sen2/export_table: Called with stream and output_config...")
    # output_table = output_config[HeaderEvent.name()] # wrong - should not hardcode event type!!
    # print(f">>> {output_table=}")
    # output_columns = [column.id for column in output_table.columns]
    # print(f">>>sen2/export_table: Output columns for table: {output_columns=}")
    for event in stream:
        print(">>>sen2/export_table: looping through events in stream - convert them to records and output to table")
        event_type = type(event)
        output_table = output_config[event_type.name()]
        output_columns = [column.id for column in output_table.columns]
        print(f">>>sen2/export_table: stream {event_type=}")
        print(">>>sen2/export_table: calling event_to_records() for given event and output_columns...")
        for record in event_to_records(event, output_columns):
            print(f">>>sen2/export_table: adding {event_type.name()} record to dataset: {record=} with {output_columns=}")
            dataset.setdefault(event_type.name(), []).append(record)
        print(f">>>sen2/export_table: about to yield {event_type.name()=}...")
        yield event
        print(">>>sen2/export_table: resuming after yielding an event")
    print(f">>>sen2/export_table: Finished input stream: returning {dataset=}")
    return dataset
 
# @generator_with_value
# def export_table(stream):
#     """
#     Collects all the records into a dictionary of lists of rows
 
#     This filter requires that the stream has been processed by `message_collector` first
 
#     :param stream: An iterator of events from message_collector
#     :yield: All events
#     :return: A dictionary of lists of rows, keyed by record name
#     """
#     dataset = {}
#     for event in stream:
#         event_type = type(event)
#         dataset.setdefault(event_type.name(), []).append(event.as_dict()["record"])
#         yield event
#     return dataset