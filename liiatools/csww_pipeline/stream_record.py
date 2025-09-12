from typing import Iterator, Optional

from more_itertools import peekable
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import generator_with_value

from liiatools.common.stream_record import HeaderEvent, _reduce_dict, text_collector


class CSWWEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "worker"
    
    pass

class HeaderEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "header"
    
    pass

class LALevelEvent(events.ParseEvent):
    @staticmethod
    def name():
        return "la_level"
    
    pass

@xml_collector
def message_collector(stream):
    """
    Collect messages from XML elements and yield results.

    :param stream: An iterator of events from an XML parser.
    :yield: Events of type HeaderEvent, CSWWEvent, LALevelEvent
    """
    stream = peekable(stream)
    assert stream.peek().tag == "Message", f"Expected 'Message', got {stream.peek.tag()}"
    while stream:
        event = stream.peek()
        if (event.get("tag") == "LALevelVacancies") | (event.get("tag") == "LALevel"):
            lalevel_record = text_collector(stream) 
            if lalevel_record:  
                yield LALevelEvent(record=lalevel_record)
        elif event.get("tag") == "CSWWWorker":
            csww_record = text_collector(stream)
            if csww_record:
                yield CSWWEvent(record=csww_record)
        elif event.get("tag") == "Header":
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
        else:
            next(stream)


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