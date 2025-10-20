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


class CSWWEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "worker"

    pass


class LALevelEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "la_level"

    pass


@xml_collector
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
        if event.get("tag") == "Header":
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
        elif event.get("tag") == "LALevelVacancies" or event.get("tag") == "LALevel":
            lalevel_record = text_collector(stream)
            if lalevel_record:
                yield LALevelEvent(record=lalevel_record)
        elif event.get("tag") == "CSWWWorker":
            csww_record = text_collector(stream)
            if csww_record:
                yield CSWWEvent(record=csww_record)
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