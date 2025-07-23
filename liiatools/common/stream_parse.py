from dagster import get_dagster_logger
from sfdata_stream_parser.events import (
    CommentNode,
    EndElement,
    ProcessingInstructionNode,
    StartElement,
    TextNode,
)

from liiatools.common.stream_errors import StreamError

log = get_dagster_logger(__name__)


try:
    from lxml import etree
except ImportError:
    pass


def dom_parse(source, filename, **kwargs):
    """
    Equivalent of the xml parse included in the sfdata_stream_parser package, but uses the ET DOM
    and allows direct DOM manipulation.

    :param source: File to be parsed
    :param filename: The name of the file
    :return: Stream of events
    """
    parser = etree.iterparse(source, events=("start", "end", "comment", "pi"), **kwargs)
    try:
        for action, elem in parser:
            if action == "start":
                yield StartElement(
                    tag=elem.tag, attrib=elem.attrib, node=elem, filename=filename
                )
                # if elem.text and elem.text.strip():
                #     yield TextNode(cell=elem.text, filename=filename, text=None)

                # with open("debug_output.txt", "a", encoding="utf-8") as f:
                #     f.write(f"StartElement: {elem.tag}, {elem.text}\n")
            elif action == "end":
                if elem.text and elem.text.strip():
                    yield TextNode(cell=elem.text, filename=filename, text=None)

                yield EndElement(tag=elem.tag, node=elem, filename=filename)
                if elem.tail:
                    yield TextNode(cell=elem.tail, filename=filename, text=None)
                
                # with open("debug_output.txt", "a", encoding="utf-8") as f:
                #     f.write(f"EndElement: {elem.tag}, {elem.text}\n")
            elif action == "comment":
                yield CommentNode(
                    cell=elem.text, node=elem, filename=filename, text=None
                )
            elif action == "pi":
                yield ProcessingInstructionNode(
                    name=elem.target,
                    cell=elem.text,
                    node=elem,
                    filename=filename,
                    text=None,
                )
            else:
                raise ValueError(f"Unknown event: {action}")
    except etree.XMLSyntaxError:
        log.info("Could not parse cin file as it is not a valid XML file")
        raise StreamError("Could not parse as file is blank")
