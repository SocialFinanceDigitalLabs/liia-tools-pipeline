from typing import Iterator, Optional

from more_itertools import peekable
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import generator_with_value

from liiatools.common.stream_record import HeaderEvent, _reduce_dict, text_collector


class HeaderEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "header"

    pass


class PersonEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "person"

    pass


class RequestsEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "requests"

    pass

class AssessmentEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "assessment"

    pass

class NamedPlanEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "named_plan"

    pass

class PlanDetailEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "plan_detail"

    pass

class ActivePlansEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "active_plan"

    pass

class PlacementDetailEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "placement_detail"

    pass

class SENneedEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "sen_need"

    pass




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
        elif event.get("tag") == "Person":
            person_record = text_collector(stream)
            if person_record:
                yield PersonEvent(record=person_record)
        elif event.get("tag") == "Requests":
            requests_record = text_collector(stream)
            if requests_record:
                yield RequestsEvent(record=requests_record)
        elif event.get("tag") == "Assessment":
            assessment_record = text_collector(stream)
            if assessment_record:
                yield AssessmentEvent(record=assessment_record)
        elif event.get("tag") == "NamedPlan":
            named_plan_record = text_collector(stream)
            if named_plan_record:
                yield NamedPlanEvent(record=named_plan_record)
        elif event.get("tag") == "PlanDetail":
            plan_detail_record = text_collector(stream)
            if plan_detail_record:
                yield PlanDetailEvent(record=plan_detail_record)
        elif event.get("tag") == "ActivePlans":
            active_plans_record = text_collector(stream)
            if active_plans_record:
                yield ActivePlansEvent(record=active_plans_record)
        elif event.get("tag") == "PlacementDetail":
            placement_detail_record = text_collector(stream)
            if placement_detail_record:
                yield PlacementDetailEvent(record=placement_detail_record)
        elif event.get("tag") == "SENneed":
            sen_need_record = text_collector(stream)
            if sen_need_record:
                yield SENneedEvent(record=sen_need_record)
        else:
            next(stream)


# @generator_with_value
# def export_table(stream, output_config):
#     """
#     Collects all the records into a dictionary of lists of rows

#     This filter requires that the stream has been processed by `message_collector` first

#     :param stream: An iterator of events from message_collector
#     :param output_config: Configuration for the output, imported as a PipelineConfig class
#     :yield: All events
#     :return: A dictionary of lists of rows, keyed by record name
#     """
#     dataset = {}
#     output_table = output_config[CINEvent.name()]
#     output_columns = [column.id for column in output_table.columns]
#     for event in stream:
#         event_type = type(event)
#         for record in event_to_records(event, output_columns):
#             dataset.setdefault(event_type.name(), []).append(record)
#         yield event
#     return dataset

@generator_with_value
def export_table(stream):
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