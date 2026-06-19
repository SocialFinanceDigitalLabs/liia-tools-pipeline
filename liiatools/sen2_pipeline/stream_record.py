from typing import Iterator, Optional
 
from more_itertools import peekable
from sfdata_stream_parser import events
from sfdata_stream_parser.collectors import xml_collector
from sfdata_stream_parser.filters.generic import generator_with_value

from liiatools.common.stream_record import _reduce_dict, text_collector #,HeaderEvent


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


@xml_collector
def person_collector(stream):
    """
    Collect all values under the <Person> element.

    Consumes the <Person> subtree, extracting text fields directly and
    delegating nested <Requests> elements to `requests_collector`.

    Parameters:
        stream (Iterator): XML parse event stream positioned at <Person>.

    Returns:
        dict: Reduced dictionary of extracted values from the Person subtree.
    """

    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "Person"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)

        if event.get("tag") in ("Requests",):
            # DELEGATION: consumes entire <Requests> subtree
            data_dict.setdefault(event.tag, []).append(requests_collector(stream))
        else:
            # direct extraction for text nodes only
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)

            # advance by one event
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def requests_collector(stream):
    """
    Collect all values under the <Requests> element.

    Consumes the <Requests> subtree, extracting text fields directly and
    delegating nested <Assessment> and <ActivePlans> elements to their
    respective collectors.

    Parameters:
        stream (Iterator): XML parse event stream positioned at <Requests>.

    Returns:
        dict: Reduced dictionary of extracted values from the Requests subtree.
    """

    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "Requests"
    while stream:
        event = stream.peek() # look without consuming
        last_tag = event.get("tag", last_tag)

        if event.get("tag") in ("Assessment",):
            # DELEGATION: consumes entire <Assessment> subtree
            data_dict.setdefault(event.tag, []).append(assessment_collector(stream))
        
        elif event.get("tag") in ("ActivePlans",):
            # DELEGATION: consumes entire <ActivePlans> subtree
            data_dict.setdefault(event.tag, []).append(activeplans_collector(stream))
        else:
            # direct extraction for text nodes only
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)

            # advance by one event
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def assessment_collector(stream):
    """
    Collect all values under the <Assessment> element.

    Consumes the <Assessment> subtree, extracting text fields directly and
    delegating nested <NamedPlan> elements to `namedplan_collector`.

    Parameters:
        stream (Iterator): XML parse event stream positioned at <Assessment>.

    Returns:
        dict: Reduced dictionary of extracted values from the Assessment subtree.
    """

    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "Assessment"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)

        if event.get("tag") in ("NamedPlan",):
            # DELEGATION: consumes entire <NamedPlan> subtree
            data_dict.setdefault(event.tag, []).append(namedplan_collector(stream))
        else:
            # direct extraction for text nodes only
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)

            # advance by one event
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def namedplan_collector(stream):
    """
    Collect all values under the <NamedPlan> element.

    Consumes the <NamedPlan> subtree, extracting text fields directly and
    using `text_collector` to fully consume and capture nested <PlanDetail> elements.

    Parameters:
        stream (Iterator): XML parse event stream positioned at <NamedPlan>.

    Returns:
        dict: Reduced dictionary of extracted values from the NamedPlan subtree.
    """

    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "NamedPlan"
    while stream:
        event = stream.peek()
        last_tag = event.get("tag", last_tag)

        if event.get("tag") in ("PlanDetail",):
            # DELEGATION: consumes entire <PlanDetail> subtree
            data_dict.setdefault(event.tag, []).append(text_collector(stream))

        else:
            # direct extraction for text nodes only
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)

            # advance by one event
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def activeplans_collector(stream):
    """
    Collect all values under the <ActivePlans> element.

    Consumes the <ActivePlans> subtree, extracting text fields directly and
    using `text_collector` to fully consume and capture nested
    <PlacementDetail> and <SENneed> elements.

    Parameters:
        stream (Iterator): XML parse event stream positioned at <ActivePlans>.

    Returns:
        dict: Reduced dictionary of extracted values from the ActivePlans subtree.
    """

    data_dict = {}
    stream = peekable(stream)
    last_tag = None
    assert stream.peek().tag == "ActivePlans"
    while stream:
        event = stream.peek() # look without consuming
        last_tag = event.get("tag", last_tag)

        if event.get("tag") in (
                "PlacementDetail",
                "SENneed",
            ):
            # DELEGATION: consumes entire <PlacementDetail> and <SENneed> subtrees
            data_dict.setdefault(event.tag, []).append(text_collector(stream))

        else:
            # direct extraction for text nodes only
            if isinstance(event, events.TextNode) and event.cell:
                data_dict.setdefault(last_tag, []).append(event.cell)

            # advance by one event
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def source_collector(stream):
    """
    Collect all text content under the <Source> element.

    Consumes the entire <Source> subtree from the XML event stream using
    `text_collector`, and returns a flattened dictionary of its contents.

    Parameters:
        stream (Iterator): XML parse event stream positioned at <Source>.

    Returns:
        dict: Reduced dictionary of values extracted from the <Source> element.
    """

    data_dict = {}
    stream = peekable(stream)
    last_tag = None

    assert stream.peek().tag == "Source"

    while stream:
        event = stream.peek() # look without consuming

        last_tag = event.get("tag", last_tag)

        if isinstance(event, events.TextNode) and event.cell:
            data_dict.setdefault(last_tag, []).append(event.cell)

        # advance by one event
        next(stream)

    print(f">>>source_collector: {_reduce_dict(data_dict)}")
    return _reduce_dict(data_dict)


def message_collector(stream):
    """
    Convert a <Message> XML stream into a sequence of structured events.

    Consumes the XML stream progressively, delegating subtree parsing to collector
    functions and yielding one event at a time (Person, Request, Assessment, etc.).

    This function is the top-level driver of the XML parsing phase. It consumes
    a stream of XML parse events (from an iterator) and yields higher-level
    domain events (e.g. PersonEvent, RequestsEvent, AssessmentEvent, etc.)
    one at a time.

    Key behaviour:
    --------------
    - The input `stream` is a forward-only iterator. This function advances
      through it progressively.
    - It uses specialised "collector" functions (e.g. person_collector,
      requests_collector) to process entire XML subtrees. These collectors
      consume all events for their corresponding tag before returning.
    - Each call to `yield` returns a single event and pauses execution until
      the next event is requested by a downstream consumer.

    Processing flow:
    ----------------
    1. The function begins at the <Message> root element.
    2. For each top-level element:
       - <Source> is parsed into a SourceEvent.
       - <Person> is parsed into a PersonEvent, and then expanded into
         multiple related events:
            - RequestsEvent
            - AssessmentEvent
            - NamedPlanEvent
            - PlanDetailEvent
            - ActivePlansEvent
            - PlacementDetailEvent
            - SENneedEvent
    3. Nested relationships (Person → Requests → Assessment → etc.) are
       flattened into separate events, with synthetic IDs added to preserve
       relationships between them.

    ID assignment:
    --------------
    Sequential IDs are generated for each entity type (Person, Request,
    Assessment, Plan, ActivePlan) to ensure referential integrity across
    emitted events.

    Generator semantics:
    --------------------
    - This function does *not* return a complete dataset.
    - Instead, it yields events lazily, one at a time.
    - Execution resumes from the last yield point when the next event
      is requested.

    Parameters:
    -----------
    stream : Iterator
        An iterator of XML parse events (already tokenised), typically wrapped
        in a peekable interface.

    Yields:
    -------
    events.ParseEvent subclasses
        Structured event objects such as:
            - SourceEvent
            - PersonEvent
            - RequestsEvent
            - AssessmentEvent
            - NamedPlanEvent
            - PlanDetailEvent
            - ActivePlansEvent
            - PlacementDetailEvent
            - SENneedEvent

    Mental model:
    -------------
    Think of this function as a "stream transformer":
        XML tokens → structured events

    Each time a caller asks for the next event, this function parses just
    enough XML to produce one event, then pauses.
    """

    stream = peekable(stream)
    assert stream.peek().tag == "Message"

    person_counter = 0
    request_counter = 0
    assessment_counter = 0
    plan_counter = 0
    active_counter = 0

    while stream:
        event = stream.peek() # look at next XML token (do not advance)

        if event.get("tag") == "Source":
            # This CALL consumes the entire <Source> subtree
            source_record = source_collector(stream)
            if source_record:
                print(f">>>message_collector: yielding SourceEvent, {source_record=}")
                yield SourceEvent(record=source_record) # yield pauses here

        elif event.get("tag") == "Person":

            # This CALL consumes the entire <Person> subtree
            person_record = person_collector(stream)

            if not person_record:
                continue # skip if collector returned nothing

            # Assign stable ID (either from XML or synthetic) LIIA - is it correct to use UPN like this? 
            
            surname = person_record.get("Surname", "").replace(" ", "").lower()
            forename = person_record.get("Forename", "").replace(" ", "").lower()

            person_birth_date = person_record.get("PersonBirthDate", "").strftime("%Y%m%d")
          
            person_id = f"{surname}_{forename}_{person_birth_date}"

            #Surname,Forename,PersonBirthDate
            person_record["PersonID"] = person_id

            # Emit top-level Person event
            print(f">>>message_collector: yielding PersonEvent, {person_record=}")
            yield PersonEvent(record=person_record)

            # Expand nested structures into flat events
            for request in _maybe_list(person_record.get("Requests")):
                request_counter += 1
                request_id = _safe_id("REQ", request_counter)

                # Propagate parent-child relationships
                request["PersonID"] = person_id
                request["RequestID"] = request_id

                yield RequestsEvent(record=request)

                # Nested: Assessment
                for assessment in _maybe_list(request.get("Assessment")):
                    assessment_counter += 1
                    assessment_id = _safe_id("ASS", assessment_counter)

                    assessment["PersonID"] = person_id
                    assessment["RequestID"] = request_id
                    assessment["AssessmentID"] = assessment_id

                    yield AssessmentEvent(record=assessment)

                    # Nested: NamedPlan
                    for plan in _maybe_list(assessment.get("NamedPlan")):
                        plan_counter += 1
                        named_plan_id = _safe_id("PLAN", plan_counter)

                        plan["PersonID"] = person_id
                        plan["RequestID"] = request_id
                        plan["AssessmentID"] = assessment_id
                        plan["NamedPlanID"] = named_plan_id

                        yield NamedPlanEvent(record=plan)

                        # Nested: PlanDetail
                        for detail in _maybe_list(plan.get("PlanDetail")):
                            detail["PersonID"] = person_id
                            detail["RequestID"] = request_id
                            detail["AssessmentID"] = assessment_id
                            detail["NamedPlanID"] = named_plan_id
                            yield PlanDetailEvent(record=detail)

                # Nested: ActivePlans
                for active in _maybe_list(request.get("ActivePlans")):
                    active_counter += 1
                    active_id = _safe_id("ACT", active_counter)

                    active["PersonID"] = person_id
                    active["RequestID"] = request_id
                    active["ActivePlanID"] = active_id

                    yield ActivePlansEvent(record=active)

                    # Nested: PlacementDetail
                    for placement in _maybe_list(active.get("PlacementDetail")):
                        placement["PersonID"] = person_id
                        placement["RequestID"] = request_id
                        placement["ActivePlanID"] = active_id
                        yield PlacementDetailEvent(record=placement)

                    # Nested: SENneed
                    for need in _maybe_list(active.get("SENneed")):
                        need["PersonID"] = person_id
                        need["RequestID"] = request_id
                        need["ActivePlanID"] = active_id
                        yield SENneedEvent(record=need)

        else:
            # advance stream by one token if not handled
            next(stream)


def _safe_id(prefix, counter):
    return f"{prefix}_{counter}"


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
    #print(f">>>sen2/_maybe_list: {value=}")
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def event_to_records(event, output_columns):
    """
    Transform a single event into one or more output records.

    Selects and orders fields from the event record based on provided output columns.

    Parameters:
        event: Event object containing a record dictionary.
        output_columns (list): Fields to include in the output.

    Yields:
        dict: A single filtered record representing the event.
    """

    record = event.record
    print(f">>>event_to_records: {record=}")
    yield {
        k: record.get(k)
        for k in output_columns
    }


@generator_with_value
def export_table(stream, output_config):
    """
    Convert a stream of events into tabular datasets.

    Consumes events produced by `message_collector`, transforms each event into
    one or more records using `event_to_records`, and stores them grouped by
    event type while yielding events unchanged.

    Parameters:
        stream (Iterator): Event stream from message_collector.
        output_config: Table configuration defining output columns.

    Yields:
        event: The original event (pass-through).

    Returns:
        dict: Mapping of event type to list of output records.
    """

    dataset = {}
    print(">>>sen2/export_table: Called with stream and output_config...")
    # output_table = output_config[event_type.name()]
    # print(f">>> {output_table=}")
    # output_columns = [column.id for column in output_table.columns]
    # print(f">>>sen2/export_table: Output columns for table: {output_columns=}")
    for event in stream:
        #print(">>>sen2/export_table: processing next event: mapping to table and extracting records")
        event_type = type(event)
        output_table = output_config[event_type.name()]
        output_columns = [column.id for column in output_table.columns]
        #print(f">>>sen2/export_table: stream {event_type=}")
        #print(">>>sen2/export_table: calling event_to_records() for given event and output_columns...")
        for record in event_to_records(event, output_columns):
            print(f">>>sen2/export_table: adding {event_type.name()} record to dataset: {record=} with {output_columns=}")
            dataset.setdefault(event_type.name(), []).append(record)
        #print(f">>>sen2/export_table: about to yield {event_type.name()=}...")
        yield event
        #print(">>>sen2/export_table: resuming after yielding an event")
    print(f">>>sen2/export_table: Finished input stream: returning {dataset=}")
    return dataset
