import unittest
from unittest import mock

from fs import open_fs

from liiatools.common.constants import SessionNames
from liiatools.common.data import FileLocator
from liiatools.common.pipeline import (
    create_session_folder,
    discover_la,
    discover_month,
    discover_year,
    move_files_for_processing,
    restore_session_folder,
)
from liiatools.common.reference import LACodeLookup
from liiatools.ssda903_pipeline.spec.samples import DIR as DIR_903


def test_create_session_folder():
    dest_fs = open_fs("mem://")
    session_folder = create_session_folder(dest_fs, SessionNames)
    assert session_folder


def test_move_files_for_processing():
    source_fs = open_fs("mem://")
    source_fs.makedir("2019")
    source_fs.writetext("2019/file1.txt", "foo")
    source_fs.writetext("2019/file2.txt", "bar")

    session_fs = open_fs("mem://")
    session_fs.makedir(SessionNames.INCOMING_FOLDER)
    file_locators = move_files_for_processing(source_fs, session_fs)
    file_locators = list(file_locators)

    assert len(file_locators) == 2

    files = list(session_fs.walk.files())
    assert len(files) == 4

    file_info = {f.meta["path"]: f for f in file_locators}

    file1_uid = file_info["/2019/file1.txt"].meta["uuid"]

    assert session_fs.exists(f"{SessionNames.INCOMING_FOLDER}/{file1_uid}")
    assert session_fs.exists(f"{SessionNames.INCOMING_FOLDER}/{file1_uid}_meta.yaml")

    assert (
        file_info["/2019/file1.txt"].meta["sha256"]
        == "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
    )

    with file_info["/2019/file1.txt"].open("rt") as FILE:
        assert FILE.read() == "foo"


def test_restore_session_folder():
    session_folder = open_fs("mem://")
    incoming_folder = session_folder.makedirs(SessionNames.INCOMING_FOLDER)
    incoming_folder.writetext(
        "UUID1_meta.yaml", "path: /2019/file1.txt\nuuid: UUID1\nsha256: sha-1"
    )
    incoming_folder.writetext("UUID1", "foo")
    incoming_folder.writetext(
        "UUID2_meta.yaml", "path: /2019/file2.txt\nuuid: UUID2\nsha256: sha-2"
    )
    incoming_folder.writetext("UUID2", "bar")

    file_locators = restore_session_folder(session_folder)
    file_info = {f.name: f for f in file_locators}

    assert file_info["/2019/file1.txt"].meta["uuid"] == "UUID1"
    assert file_info["/2019/file2.txt"].meta["uuid"] == "UUID2"
    assert file_info["/2019/file1.txt"].meta["sha256"] == "sha-1"
    assert file_info["/2019/file2.txt"].meta["sha256"] == "sha-2"

    with file_info["/2019/file1.txt"].open("rt") as FILE:
        assert FILE.read() == "foo"


def test_discover_year_no_year():
    samples_fs = open_fs(DIR_903.as_posix())
    locator = FileLocator(samples_fs, "SSDA903_episodes.csv.xlsx")
    assert discover_year(locator) is None


def test_discover_year_dir_year():
    samples_fs = open_fs(DIR_903.as_posix())
    locator = FileLocator(
        samples_fs, "SSDA903_episodes.csv", original_path="/2020/SSDA903_episodes.csv"
    )
    assert discover_year(locator) == 2020


def test_discover_year_file_year():
    samples_fs = open_fs(DIR_903.as_posix())
    locator = FileLocator(
        samples_fs,
        "SSDA903_episodes.csv",
        original_path="/SSDA903_2022-23_episodes.csv",
    )
    assert discover_year(locator) == 2023


def test_discover_year_dir_and_file_year():
    samples_fs = open_fs(DIR_903.as_posix())
    locator = FileLocator(
        samples_fs,
        "SSDA903_episodes.csv",
        original_path="/2021/SSDA903_2022-23_episodes.csv",
    )
    assert discover_year(locator) == 2021


def test_discover_month():
    samples_fs = open_fs(DIR_903.as_posix())
    locator_file = FileLocator(
        samples_fs,
        "SSDA903_2020_episodes.csv",
        original_path="/2020/SSDA903_2020_jan_episodes.csv",
    )
    assert discover_month(locator_file) == "jan"

    locator_dir = FileLocator(
        samples_fs,
        "SSDA903_2020_episodes.csv",
        original_path="/2020_feb/SSDA903_2020_episodes.csv",
    )
    assert discover_month(locator_dir) == "feb"

    locator_dir_and_file = FileLocator(
        samples_fs,
        "SSDA903_2020_episodes.csv",
        original_path="/2020_feb/SSDA903_2020_jan_episodes.csv",
    )
    assert discover_month(locator_dir) == "feb"

    locator_no_month = FileLocator(samples_fs, "SSDA903_2020_episodes.csv")
    assert discover_month(locator_no_month) is None


class TestDiscoverLA(unittest.TestCase):
    @mock.patch.object(LACodeLookup, "codes", new_callable=mock.PropertyMock)
    def test_discover_la_from_directory(self, mock_codes):
        mock_codes.return_value = ["873", "883", "HAC"]

        file_locator = FileLocator(
            open_fs(DIR_903.as_posix()),
            "path/to/Fons-a821f-Cambridgeshire-873/file.csv",
        )

        result = discover_la(file_locator)
        self.assertEqual(result, "873")

    @mock.patch.object(LACodeLookup, "codes", new_callable=mock.PropertyMock)
    def test_discover_la_from_filename(self, mock_codes):
        mock_codes.return_value = ["873", "883", "HAC"]

        file_locator = FileLocator(
            open_fs(DIR_903.as_posix()),
            "path/to/Fons-04cd3-Thurrock-883/file.csv",
        )

        result = discover_la(file_locator)
        self.assertEqual(result, "883")

    @mock.patch.object(LACodeLookup, "codes", new_callable=mock.PropertyMock)
    def test_discover_la_no_match(self, mock_codes):
        mock_codes.return_value = ["873", "883", "HAC"]

        file_locator = FileLocator(
            open_fs(DIR_903.as_posix()),
            "path/to/unknown_file.csv",
            original_path="/path/to/unknown_file.csv",
        )

        result = discover_la(file_locator)
        self.assertEqual(result, None)
