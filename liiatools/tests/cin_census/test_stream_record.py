import unittest
from datetime import date

from sfdata_stream_parser.events import EndElement, StartElement, TextNode

from liiatools.cin_census_pipeline.stream_record import (
    CINEvent,
    HeaderEvent,
    _maybe_list,
    child_collector,
    cin_collector,
    cin_event,
    event_to_records,
    export_table,
    message_collector,
)
from liiatools.common.data import PipelineConfig

output_config = PipelineConfig(
    sensor_trigger={"move_current_org_sensor": False, "move_concat_sensor": False},
    retention_columns={"year_column": "Year", "la_column": "LA"},
    retention_period={"PAN": 12},
    degrade_at_clean={"PAN": True},
    reports_to_shared={"PAN": True},
    la_signed={
        "Barking and Dagenham": {"PAN": "Yes"},
    },
    table_list=[
        {
            "id": "cin",
            "retain": ["PAN"],
            "columns": [
                {
                    "id": "LAchildID",
                    "type": "string",
                    "unique_key": True,
                    "enrich": ["integer", "add_la_suffix"],
                },
                {"id": "Date", "type": "date", "unique_key": True},
                {"id": "Type", "type": "string", "unique_key": True},
                {"id": "CINreferralDate", "type": "date"},
                {"id": "ReferralSource", "type": "category", "unique_key": True},
                {"id": "PrimaryNeedCode", "type": "category"},
                {"id": "CINclosureDate", "type": "date"},
                {"id": "ReasonForClosure", "type": "category"},
                {"id": "DateOfInitialCPC", "type": "date"},
                {"id": "ReferralNFA", "type": "category"},
                {"id": "CINPlanStartDate", "type": "date"},
                {"id": "CINPlanEndDate", "type": "date"},
                {"id": "S47ActualStartDate", "type": "date"},
                {"id": "InitialCPCtarget", "type": "date", "sort": 2},
                {"id": "ICPCnotRequired", "type": "category"},
                {"id": "AssessmentActualStartDate", "type": "date"},
                {"id": "AssessmentInternalReviewDate", "type": "date", "sort": 1},
                {"id": "AssessmentAuthorisationDate", "type": "date"},
                {"id": "Factors", "type": "category"},
                {"id": "CPPstartDate", "type": "date"},
                {"id": "CPPendDate", "type": "date"},
                {"id": "InitialCategoryOfAbuse", "type": "category"},
                {"id": "LatestCategoryOfAbuse", "type": "category"},
                {"id": "NumberOfPreviousCPP", "type": "numeric"},
                {"id": "UPN", "type": "string"},
                {"id": "FormerUPN", "type": "string"},
                {"id": "UPNunknown", "type": "category"},
                {
                    "id": "PersonBirthDate",
                    "type": "date",
                    "degrade": "first_of_month",
                },
                {
                    "id": "ExpectedPersonBirthDate",
                    "type": "date",
                    "degrade": "first_of_month",
                },
                {"id": "GenderCurrent", "type": "category"},
                {
                    "id": "PersonDeathDate",
                    "type": "date",
                    "degrade": "first_of_month",
                },
                {
                    "id": "PersonSchoolYear",
                    "type": "numeric",
                    "enrich": "school_year",
                },
                {"id": "Ethnicity", "type": "category"},
                {"id": "Disabilities", "type": "category"},
                {"id": "LA", "type": "string", "enrich": "la_name"},
                {"id": "Year", "type": "numeric", "enrich": "year", "sort": 0},
            ],
        }
    ],
)

output_table = output_config["cin"]
output_columns = [column.id for column in output_table.columns]


def test_maybe_list():
    assert _maybe_list(None) == []
    assert _maybe_list(42) == [42]
    assert _maybe_list([1, 2, 3]) == [1, 2, 3]


def test_cin_event():
    record = {"Name": "John", "DOB": "2000-01-01"}
    property = "DOB"
    export_headers = ["DOB", "Date", "Type"]
    event_name = "Date of Birth"
    assert cin_event(record, property, export_headers, event_name=event_name) == (
        {"DOB": "2000-01-01", "Date": "2000-01-01", "Type": "Date of Birth"},
    )
    assert cin_event(record, "UnknownProperty", export_headers) == ()


def test_event_to_records():
    cin_record = {
        "ChildIdentifiers": {
            "LAchildID": "DfEX0000001",
            "UPN": "A123456789123",
            "PersonBirthDate": "2004-03-24",
        },
        "ChildCharacteristics": {"Ethnicity": "WBRI"},
        "CINdetails": {
            "CINreferralDate": "2009-03-15",
        },
    }

    assert list(
        event_to_records(CINEvent(record=cin_record), output_columns=output_columns)
    ) == [
        {
            "LAchildID": "DfEX0000001",
            "Date": "2009-03-15",
            "Type": "CINreferralDate",
            "CINreferralDate": "2009-03-15",
            "ReferralSource": None,
            "PrimaryNeedCode": None,
            "CINclosureDate": None,
            "ReasonForClosure": None,
            "DateOfInitialCPC": None,
            "ReferralNFA": None,
            "CINPlanStartDate": None,
            "CINPlanEndDate": None,
            "S47ActualStartDate": None,
            "InitialCPCtarget": None,
            "ICPCnotRequired": None,
            "AssessmentActualStartDate": None,
            "AssessmentInternalReviewDate": None,
            "AssessmentAuthorisationDate": None,
            "Factors": None,
            "CPPstartDate": None,
            "CPPendDate": None,
            "InitialCategoryOfAbuse": None,
            "LatestCategoryOfAbuse": None,
            "NumberOfPreviousCPP": None,
            "UPN": "A123456789123",
            "FormerUPN": None,
            "UPNunknown": None,
            "PersonBirthDate": "2004-03-24",
            "ExpectedPersonBirthDate": None,
            "GenderCurrent": None,
            "PersonDeathDate": None,
            "PersonSchoolYear": None,
            "Ethnicity": "WBRI",
            "Disabilities": "",
            "LA": None,
            "Year": None,
        },
    ]


class TestRecord(unittest.TestCase):
    def generate_text_element(self, tag: str, cell):
        """
        Create a complete TextNode sandwiched between a StartElement and EndElement

        :param tag: XML tag
        :param cell: text to be stored in the given XML tag, could be a string, integer, float etc.
        :return: StartElement and EndElement with given tags and TextNode with given text
        """
        yield StartElement(tag=tag)
        yield TextNode(cell=str(cell), text=None)
        yield EndElement(tag=tag)

    def generate_test_child(self):
        """
        Generate a sample child

        :return: Stream of generators containing information required to create a child
        """
        yield StartElement(tag="Child")
        yield StartElement(tag="ChildIdentifiers")
        yield from self.generate_text_element(tag="LAchildID", cell="DfEX0000001")
        yield from self.generate_text_element(tag="UPN", cell="A123456789123")
        yield from self.generate_text_element(
            tag="PersonBirthDate", cell=date(2004, 3, 24)
        )
        yield EndElement(tag="ChildIdentifiers")
        yield StartElement(tag="ChildCharacteristics")
        yield from self.generate_text_element(tag="Ethnicity", cell="WBRI")
        yield EndElement(tag="ChildCharacteristics")
        yield StartElement(tag="CINdetails")
        yield from self.generate_text_element(
            tag="CINreferralDate", cell=date(2009, 3, 15)
        )
        yield from self.generate_text_element(tag="ReferralSource", cell="1A")
        yield from self.generate_text_element(tag="ReferralNFA", cell="false")
        yield StartElement(tag="Assessments")
        yield from self.generate_text_element(
            tag="AssessmentActualStartDate", cell=date(2009, 2, 21)
        )
        yield EndElement(tag="Assessments")
        yield StartElement(tag="Section47")
        yield from self.generate_text_element(
            tag="S47ActualStartDate", cell=date(2009, 2, 17)
        )
        yield EndElement(tag="Section47")
        yield StartElement(tag="ChildProtectionPlans")
        yield from self.generate_text_element(tag="NumberOfPreviousCPP", cell=10)
        yield EndElement(tag="ChildProtectionPlans")
        yield EndElement(tag="CINdetails")
        yield EndElement(tag="Child")

    def generate_test_cin_census_file(self):
        """
        Generate a sample children in need census file

        :return: Stream of generators containing information required to create an XML file
        """
        yield StartElement(tag="Message")
        yield StartElement(tag="Header")
        yield from self.generate_text_element(tag="Version", cell=1)
        yield EndElement(tag="Header")
        yield from self.generate_test_child()
        yield EndElement(tag="Message")

    def test_cin_collector(self):
        test_stream = self.generate_test_cin_census_file()
        test_event = cin_collector(test_stream)
        self.assertEqual(
            test_event,
            {
                "Version": "1",
                "LAchildID": "DfEX0000001",
                "UPN": "A123456789123",
                "PersonBirthDate": "2004-03-24",
                "Ethnicity": "WBRI",
                "CINreferralDate": "2009-03-15",
                "ReferralSource": "1A",
                "ReferralNFA": "false",
                "Assessments": {"AssessmentActualStartDate": "2009-02-21"},
                "Section47": {"S47ActualStartDate": "2009-02-17"},
                "ChildProtectionPlans": {"NumberOfPreviousCPP": "10"},
            },
        )

    def test_child_collector(self):
        test_stream = self.generate_test_child()
        test_event = child_collector(test_stream)
        self.assertEqual(
            test_event,
            {
                "ChildIdentifiers": {
                    "LAchildID": "DfEX0000001",
                    "UPN": "A123456789123",
                    "PersonBirthDate": "2004-03-24",
                },
                "ChildCharacteristics": {"Ethnicity": "WBRI"},
                "CINdetails": {
                    "CINreferralDate": "2009-03-15",
                    "ReferralSource": "1A",
                    "ReferralNFA": "false",
                    "Assessments": {"AssessmentActualStartDate": "2009-02-21"},
                    "Section47": {"S47ActualStartDate": "2009-02-17"},
                    "ChildProtectionPlans": {"NumberOfPreviousCPP": "10"},
                },
            },
        )

    def test_message_collector(self):
        test_stream = self.generate_test_cin_census_file()
        test_events = list(message_collector(test_stream))
        self.assertEqual(len(test_events), 2)
        self.assertIsInstance(test_events[0], HeaderEvent)
        self.assertEqual(test_events[0].record, {"Version": "1"})
        self.assertIsInstance(test_events[1], CINEvent)
        self.assertEqual(
            test_events[1].record,
            {
                "ChildIdentifiers": {
                    "LAchildID": "DfEX0000001",
                    "UPN": "A123456789123",
                    "PersonBirthDate": "2004-03-24",
                },
                "ChildCharacteristics": {"Ethnicity": "WBRI"},
                "CINdetails": {
                    "CINreferralDate": "2009-03-15",
                    "ReferralSource": "1A",
                    "ReferralNFA": "false",
                    "Assessments": {"AssessmentActualStartDate": "2009-02-21"},
                    "Section47": {"S47ActualStartDate": "2009-02-17"},
                    "ChildProtectionPlans": {"NumberOfPreviousCPP": "10"},
                },
            },
        )

    def test_export_table(self):
        test_stream = self.generate_test_cin_census_file()
        test_events = list(message_collector(test_stream))
        dataset_holder, stream = export_table(test_events, output_config=output_config)

        self.assertEqual(len(list(stream)), 2)

        data = dataset_holder.value
        self.assertEqual(len(data), 1)
        self.assertEqual(
            data["cin"],
            [
                {
                    "LAchildID": "DfEX0000001",
                    "Date": "2009-03-15",
                    "Type": "CINreferralDate",
                    "CINreferralDate": "2009-03-15",
                    "ReferralSource": "1A",
                    "PrimaryNeedCode": None,
                    "CINclosureDate": None,
                    "ReasonForClosure": None,
                    "DateOfInitialCPC": None,
                    "ReferralNFA": "false",
                    "CINPlanStartDate": None,
                    "CINPlanEndDate": None,
                    "S47ActualStartDate": None,
                    "InitialCPCtarget": None,
                    "ICPCnotRequired": None,
                    "AssessmentActualStartDate": None,
                    "AssessmentInternalReviewDate": None,
                    "AssessmentAuthorisationDate": None,
                    "Factors": None,
                    "CPPstartDate": None,
                    "CPPendDate": None,
                    "InitialCategoryOfAbuse": None,
                    "LatestCategoryOfAbuse": None,
                    "NumberOfPreviousCPP": None,
                    "UPN": "A123456789123",
                    "FormerUPN": None,
                    "UPNunknown": None,
                    "PersonBirthDate": "2004-03-24",
                    "ExpectedPersonBirthDate": None,
                    "GenderCurrent": None,
                    "PersonDeathDate": None,
                    "PersonSchoolYear": None,
                    "Ethnicity": "WBRI",
                    "Disabilities": "",
                    "LA": None,
                    "Year": None,
                },
                {
                    "LAchildID": "DfEX0000001",
                    "Date": "2009-02-21",
                    "Type": "AssessmentActualStartDate",
                    "CINreferralDate": "2009-03-15",
                    "ReferralSource": "1A",
                    "PrimaryNeedCode": None,
                    "CINclosureDate": None,
                    "ReasonForClosure": None,
                    "DateOfInitialCPC": None,
                    "ReferralNFA": "false",
                    "CINPlanStartDate": None,
                    "CINPlanEndDate": None,
                    "S47ActualStartDate": None,
                    "InitialCPCtarget": None,
                    "ICPCnotRequired": None,
                    "AssessmentActualStartDate": "2009-02-21",
                    "AssessmentInternalReviewDate": None,
                    "AssessmentAuthorisationDate": None,
                    "Factors": "",
                    "CPPstartDate": None,
                    "CPPendDate": None,
                    "InitialCategoryOfAbuse": None,
                    "LatestCategoryOfAbuse": None,
                    "NumberOfPreviousCPP": None,
                    "UPN": "A123456789123",
                    "FormerUPN": None,
                    "UPNunknown": None,
                    "PersonBirthDate": "2004-03-24",
                    "ExpectedPersonBirthDate": None,
                    "GenderCurrent": None,
                    "PersonDeathDate": None,
                    "PersonSchoolYear": None,
                    "Ethnicity": "WBRI",
                    "Disabilities": "",
                    "LA": None,
                    "Year": None,
                },
                {
                    "LAchildID": "DfEX0000001",
                    "Date": "2009-02-17",
                    "Type": "S47ActualStartDate",
                    "CINreferralDate": "2009-03-15",
                    "ReferralSource": "1A",
                    "PrimaryNeedCode": None,
                    "CINclosureDate": None,
                    "ReasonForClosure": None,
                    "DateOfInitialCPC": None,
                    "ReferralNFA": "false",
                    "CINPlanStartDate": None,
                    "CINPlanEndDate": None,
                    "S47ActualStartDate": "2009-02-17",
                    "InitialCPCtarget": None,
                    "ICPCnotRequired": None,
                    "AssessmentActualStartDate": None,
                    "AssessmentInternalReviewDate": None,
                    "AssessmentAuthorisationDate": None,
                    "Factors": None,
                    "CPPstartDate": None,
                    "CPPendDate": None,
                    "InitialCategoryOfAbuse": None,
                    "LatestCategoryOfAbuse": None,
                    "NumberOfPreviousCPP": None,
                    "UPN": "A123456789123",
                    "FormerUPN": None,
                    "UPNunknown": None,
                    "PersonBirthDate": "2004-03-24",
                    "ExpectedPersonBirthDate": None,
                    "GenderCurrent": None,
                    "PersonDeathDate": None,
                    "PersonSchoolYear": None,
                    "Ethnicity": "WBRI",
                    "Disabilities": "",
                    "LA": None,
                    "Year": None,
                },
            ],
        )
