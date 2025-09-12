# Taken pink line bits from CIN stream pipeline

from typing import Iterator, Optional

from more_itertools import peekable
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import generator_with_value

from liiatools.common.stream_record import HeaderEvent, _reduce_dict, text_collector


# Now start using instructions for CSWW to put together next section
# Let's make a class for each CSWW event
# CSWWEvent class will have traits in there from events.ParseEvent class
# Python staticmethod 
# def is used to define a function
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


# Now let's make a message collector from XML elements
# doc strings use 3 quotation marks; add arguments/parameters underneath first line of words you type in
# four loops iterate over things; an iterator is something you can go over piece by piece
# yield is what comes out
# Q for PT - Is the decorator on line below even needed?
~xml_collector
def message_collector(stream): 
    """
    Collect messages from XML elements and yield results.

    :param stream: An iterator of events from an XML parser
    :yield: Events of type HeaderEvent, CSWWEvent, LALevelEvent
    """
    stream = peekable(stream)
    # Check LHS on line below is exactly equivalent to RHS; bit after comma will tell us error (stream.peek.tag)
    assert stream.peek().tag == "Message", f"Expected 'Message', got{stream.peek.tag}"
    # If line above comes up false, it won't be doing the while below, otherwise it'll keep doing it
    while stream:
        event = stream.peek()
        # | means 'pipe' or 'or'
        if (event.get("tag") == "LALevelVacancies") | (event.get("tag") == "LALevel"):
            # If line above is true (one or both bits are true) then do the following line
            # Line below means it's going to put it into la_level_record
            la_level_record = text_collector(stream)
            # !=None means True
            if la_level_record:
                yield LALevelEvent(record=la_level_record)
        # If above isn't the case then do elif (else if) (saves having to do if again and again)
        elif event.get("tag") == "CSWWWorker":
            worker_record = text_collector(stream)
            if worker_record:
                yield CSWWEvent(record=worker_record)
        elif event.get("tag") == "Header":
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
        else:
            next(stream)
# We've now separated into 3 tables

# Now we need to export the table
@generator_with_value
def export_table(stream)
# E.g. for CSWWWORKER, the key is the tag, and its value is the value
    """
    Collects all the records into a dictionary of lists of rows

    This filter requires that the stream has been processed by `message_collector` first

    :param stream: An iterator of events from message_collector
    :yield: All events
    :return: A dictionary of lists of rows, keyed by record name
    """
    # Let's make an empty dictionary called 'dataset'
    dataset = {}
    for event in stream:
        # Check which event of the 3 types it is (Header etc)
        event_type = type(event)
        # setdefault is a different way to make a dictionary
        # [] means empty list
        # append lets you add record from the current event
        # as.dict - treat the event as a dictionary
        # Last bit of line below means let's take the record out of it
        # We'll end up with separate dictionaries for each worker, and lalevelvacancies
        # Link to work we've been doing on CIN results - each child is a row of its own and all details come up in columns
        dataset.setdefault(event_type.name(),[]).append(event.as_dict()["record"])
        yield event
    return dataset