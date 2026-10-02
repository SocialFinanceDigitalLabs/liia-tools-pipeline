import dagster as dg

class HashingSecretResource(dg.ConfigurableResource):
    key: str
    version: str

hashing_secret = HashingSecretResource(
    key=dg.EnvVar("HASH_SECRET_KEY"),
    version=dg.EnvVar("HASH_SECRET_VERSION"),
)