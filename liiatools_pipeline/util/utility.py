import fs.errors
from dagster import get_dagster_logger

log = get_dagster_logger(__name__)


def opendir_location(folder_asset, folder_location):
    """
    Open a folder location using the folder asset with error catching.

    :param folder_asset: The folder asset to use to open the folder location.
    :param folder_location: The folder location to open.
    :return: The directory pointer to the folder location.
    """
    try:
        dir_pointer = folder_asset.opendir(folder_location)
        log.info(f"Opening folder location: {folder_location}")
    except fs.errors.ResourceNotFound:
        log.error(f"Failed to open folder location: {folder_location}")
        dir_pointer = None

    return dir_pointer
