import unittest
import os
from pathlib import Path
from unittest.mock import MagicMock


def workspace_folder():
    return Path(
        "C:/Users/disha.javur/OneDrive - Social Finance Ltd/Desktop/GitHUb/liia-python/liiatools_pipeline/sample_output/temp_dir/workspace"
    )


def shared_folder():
    return Path(
        "C:/Users/disha.javur/OneDrive - Social Finance Ltd/Desktop/GitHUb/liia-python/liiatools_pipeline/sample_output/temp_dir/shared"
    )


class DataframeArchive:
    def __init__(self, data):
        self.data = data

    def current(self, la_code):
        return self.data.get(la_code, None)


class MockData:
    def export(self, folder, filename, format):
        # Mock export function for testing
        with open(folder / f"{filename}.{format}", "w") as f:
            f.write(f"Mock data for {filename}")


# Function to create concatenated view
def create_concatenated_view(current: DataframeArchive, temp_shared: Path):
    concat_folder = temp_shared / "concatenated"
    concat_folder.mkdir(parents=True, exist_ok=True)

    authorities = ["822", "823"]  # Mocked list of authorities for this example

    for la_code in authorities:
        concat_data = current.current(la_code)
        if concat_data:
            concat_data.export(concat_folder, f"{la_code}_dataset_", "csv")


class TestConcatenateFiles(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory and file paths
        temp_workspace = workspace_folder()
        temp_shared = shared_folder()

        self.temp_dir = Path("temp_test_dir")
        self.temp_dir.mkdir(exist_ok=True)

        self.temp_shared = self.temp_dir / "shared"
        self.temp_shared.mkdir(exist_ok=True)

        # Create mock data and DataframeArchive
        mock_data = {"822": MockData(), "823": MockData()}
        self.current = DataframeArchive(mock_data)

    # def tearDown(self):
    #     # Clean up the temporary directory and files
    #     for item in self.temp_dir.glob('**/*'):
    #         if item.is_file():
    #             item.unlink()
    #         elif item.is_dir():
    #             item.rmdir()
    #     self.temp_dir.rmdir()

    def test_create_concatenated_view(self):
        # Run the function
        create_concatenated_view(self.current, self.temp_shared)

        # Verify that the concatenated files were created
        concat_folder = self.temp_shared / "concatenated"
        self.assertTrue((concat_folder / "822_dataset_.csv").exists())
        self.assertTrue((concat_folder / "823_dataset_.csv").exists())

        with open(concat_folder / "822_dataset_.csv", "r") as f:
            content = f.read()
        self.assertEqual(content, "Mock data for 822_dataset_")

        with open(concat_folder / "823_dataset_.csv", "r") as f:
            content = f.read()
        self.assertEqual(content, "Mock data for 823_dataset_")


if __name__ == "__main__":
    unittest.main()
