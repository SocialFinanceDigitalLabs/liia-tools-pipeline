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


class HeaderEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "Header"

    pass


class PersonEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "person"

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
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)
        if event.get("tag") in (
            "person",
            "requests",
            "assessment",
            "NamedPlan",
            "PlanDetail",
            "ActivePlans",
            "PlacementDetail",
            "SENneed",
        ):
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
        else:
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)
            next(stream)

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
    assert stream.peek().tag == "person"
    while stream:
        event = stream.peek()
        if event.get("tag") in ("person"):
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
#        elif event.get("tag") == "CINdetails":
#            data_dict.setdefault(event.tag, []).append(cin_collector(stream))
        else:
            next(stream)

    return _reduce_dict(data_dict)






def message_collector(stream):
    """
    Collect messages from XML elements and yield events

    :param stream: An iterator of events from an XML parser
    :yield: Events of type HeaderEvent, PersonEvent, RequestsEvent, AssessmentEvent, NamedPlanEvent, PlanDetailEvent, ActivePlansEvent, PlacementDetailEvent, or SENneedEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", f"Expected Message, got {stream.peek().tag}"
    while stream:
        event = stream.peek()
        if event.get("tag") == "Header":
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
        elif event.get("tag") == "person":
            person_record = text_collector(stream)
            if person_record:
                yield PersonEvent(record=person_record)
        # elif event.get("tag") == "Requests":
        #     requests_record = text_collector(stream)
        #     if requests_record:
        #         yield RequestsEvent(record=requests_record)
        # elif event.get("tag") == "Assessment":
        #     assessment_record = text_collector(stream)
        #     if assessment_record:
        #         yield AssessmentEvent(record=assessment_record)
        # elif event.get("tag") == "NamedPlan":
        #     named_plan_record = text_collector(stream)
        #     if named_plan_record:
        #         yield NamedPlanEvent(record=named_plan_record)
        # elif event.get("tag") == "PlanDetail":
        #     plan_detail_record = text_collector(stream)
        #     if plan_detail_record:
        #         yield PlanDetailEvent(record=plan_detail_record)
        # elif event.get("tag") == "ActivePlans":
        #     active_plans_record = text_collector(stream)
        #     if active_plans_record:
        #         yield ActivePlansEvent(record=active_plans_record)
        # elif event.get("tag") == "PlacementDetail":
        #     placement_detail_record = text_collector(stream)
        #     if placement_detail_record:
        #         yield PlacementDetailEvent(record=placement_detail_record)
        # elif event.get("tag") == "SENneed":
        #     sen_need_record = text_collector(stream)
        #     if sen_need_record:
        #         yield SENneedEvent(record=sen_need_record)
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
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def event_to_records(event: PersonEvent, output_columns: list) -> Iterator[dict]:
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
    record = event.record
    child = {
        **record.get("ChildIdentifiers", {}),
        **record.get("ChildCharacteristics", {}),
    }
    child["Disabilities"] = ",".join(_maybe_list(child.get("Disability")))

    for cin_item in _maybe_list(record.get("CINdetails")):
        yield from cin_event(
            {**child, **cin_item}, "CINreferralDate", export_headers=output_columns
        )
        yield from cin_event(
            {**child, **cin_item}, "CINclosureDate", export_headers=output_columns
        )

        for assessment in _maybe_list(cin_item.get("Assessments")):
            assessment["Factors"] = ",".join(
                _maybe_list(assessment.get("AssessmentFactors"))
            )
            yield from cin_event(
                {**child, **cin_item, **assessment},
                "AssessmentActualStartDate",
                export_headers=output_columns,
            )
            yield from cin_event(
                {**child, **cin_item, **assessment},
                "AssessmentAuthorisationDate",
                export_headers=output_columns,
            )

        for cin in _maybe_list(cin_item.get("CINPlanDates")):
            yield from cin_event(
                {**child, **cin_item, **cin},
                "CINPlanStartDate",
                export_headers=output_columns,
            )
            yield from cin_event(
                {**child, **cin_item, **cin},
                "CINPlanEndDate",
                export_headers=output_columns,
            )

        for s47 in _maybe_list(cin_item.get("Section47")):
            yield from cin_event(
                {**child, **cin_item, **s47},
                "S47ActualStartDate",
                export_headers=output_columns,
            )

        for cpp in _maybe_list(cin_item.get("ChildProtectionPlans")):
            yield from cin_event(
                {**child, **cin_item, **cpp},
                "CPPstartDate",
                export_headers=output_columns,
            )
            yield from cin_event(
                {**child, **cin_item, **cpp},
                "CPPendDate",
                export_headers=output_columns,
            )
            for cpp_review in _maybe_list(cpp.get("CPPreviewDate")):
                cpp_review = {"CPPreviewDate": cpp_review}
                yield from cin_event(
                    {**child, **cin_item, **cpp, **cpp_review},
                    "CPPreviewDate",
                    export_headers=output_columns,
                )





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


    output_table = output_config[PersonEvent.name()]
    output_columns = [column.id for column in output_table.columns]
    for event in stream:
        event_type = type(event)
        for record in event_to_records(event, output_columns):
            dataset.setdefault(event_type.name(), []).append(record)
        yield event
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