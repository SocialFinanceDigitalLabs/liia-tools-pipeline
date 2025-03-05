import importlib.resources
import logging
import yaml

from decouple import config as env_config

logger = logging.getLogger(__name__)

region_config = env_config("REGION_CONFIG", cast=str)


class LACodeLookup:
    def __init__(self):
        try:
            with importlib.resources.open_text(
                f"{region_config}_pipeline_config", "_authorities.yml"
            ) as FILE:
                self.__mappings = yaml.safe_load(FILE)
        except ModuleNotFoundError:
            logger.info(f"Configuration region '{region_config}' not found.")
        except FileNotFoundError:
            logger.info(
                f"Configuration file '_authorities.yml' not found in '{region_config}'."
            )

        assert (
            "data_codes" in self.__mappings
        ), "LA-codes.yml does not contain data_codes"
        self.__mappings = self.__mappings["data_codes"]

        self.__codes = {v: k for k, v in self.__mappings.items()}
        self.__names = {k: v for k, v in self.__mappings.items()}

        assert len(self.__codes) == len(self.__names), "Duplicate LA codes or names"

    def __getitem__(self, item):
        if item in self.__codes:
            return self.__codes[item]

        if item in self.__names:
            return self.__names[item]

        raise KeyError(f"Unknown LA code or name: {item}")

    def get_by_name(self, name):
        return self.__names[name]

    def get_by_code(self, code):
        return self.__codes[code]

    @property
    def names(self):
        return self.__names.keys()

    @property
    def codes(self):
        return self.__codes.keys()
