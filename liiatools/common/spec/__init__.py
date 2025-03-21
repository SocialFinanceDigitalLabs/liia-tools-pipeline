import decouple
from decouple import config as env_config

def load_region_env():
    try:
        return env_config("REGION_CONFIG", cast=str)
    except decouple.UndefinedValueError:
        raise decouple.UndefinedValueError("REGION_CONFIG not defined in .env file")